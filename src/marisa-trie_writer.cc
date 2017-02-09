#include "marisa-trie_writer.h"

#include <cmath>

#include <snappy.h>
#include <glog/logging.h>

#include "marisa/trie.h"
#include "marisa/keyset.h"

#include "utils/varint.h"
#include "utils/pfordelta.h"
#include "utils/timestamp.h"
#include "utils/file_util.h"
#include "utils/file_stream.h"

namespace scdb {

const static char* kVersion = "SCDBV2.";

class MarisaTrieWriter::Impl
{
public:
    Impl(const Writer::Option& option, const std::string& fname)
        : option_(option),
          fname_(fname),
          done_(false)
    {
    }

    ~Impl()
    {
        Close();
    }

    void Put(const StringPiece& k)
    {
        DCHECK(option_.build_type==1) << "Expect Build without value";

        auto len = k.length();
        if (len == 0)
            return ;

        marisa::Key key;
        key.set_str(k.data(), len);
        keyset_.push_back(key);
        key_counts_[len]++;
    }

    void PutTogether(const StringPiece& k, const StringPiece& v)
    {
        if (k.length() == 0)
            return ;

        std::string ktv(k.ToString());
        ktv.append("\t");
        ktv.append(v.ToString());

        marisa::Key key;
        key.set_str(ktv.data(), ktv.length());
        keyset_.push_back(key);
        key_counts_[k.length()]++;
    }

    void Put(const StringPiece& k, const StringPiece& v)
    {
        DCHECK(option_.build_type==0) << "Expect Build with value";

        auto len = k.length();
        if (len == 0)
            return ;

        if (option_.compress_type == 2)
        {
            PutTogether(k, v);
            return ;
        }
        ResizeData(len);

        int64_t data_length = data_lengths_[len];
        if (EqualLastValue(len, v))
        {
            data_length -= last_values_lengths_[len];
        }
        else
        {
            auto dos = GetDataStream(len);

            size_t encode_length = 0;
            size_t value_length = v.length();
            if (option_.compress_type == 1)
            {
                std::string cv;
                snappy::Compress(v.data(), v.length(), &cv);
                value_length = cv.length();

                encode_length = EncodeVarint(cv.length(), dos);
                dos->Append(cv);
            }
            else
            {
                encode_length = EncodeVarint(v.length(), dos);
                dos->Append(v);
            }

            data_lengths_[len] += encode_length + value_length;

            last_values_[len] = v.ToString();
            last_values_lengths_[len] = value_length;
        }

        marisa::Key key;
        key.set_str(k.data(), len);
        keyset_.push_back(key);
        offsets_.push_back(data_length);
        key_counts_[len]++;
    }

    void Close()
    {
        if (done_)
            return ;

        for (size_t i = 0; i < data_streams_.size(); i++)
        {
            if (data_streams_[i])
            {
                data_streams_[i]->Close();
            }
        }
    
        std::vector<std::string> files;

        // we must build index first
        auto trie_file = BuildTrie(); // Must build trie first
        auto pfd_file = BuildPFD();

        std::string metadata_file = option_.temp_folder + "metadata.dat";
        WriteMetaData(metadata_file, pfd_file, trie_file);

        files.push_back(metadata_file);
        files.push_back(pfd_file);
        files.push_back(trie_file); // let trie closed to data, they will mmape together

        for (auto& file : data_files_)
        {
            if (!file.empty())
            {
                files.push_back(file);
            }
        }
    
        MergeFiles(files);
        Cleanup(files);
    }
    
    void WriteMetaData(const std::string& fname, const std::string& pfd_file, const std::string& trie_file)
    {
        FileOutputStream os(fname);
    
        // WriteVersion
        os.Append(kVersion);
    
        // Write Time
        auto now = Timestamp::Now();
        os.AppendInt64(now.MicroSecondsSinceEpoch());

        os.AppendInt32(GetNumKeyCount());
        os.AppendInt32(key_counts_.size()-1);

        LOG(INFO) << "num key count " << GetNumKeyCount();
        LOG(INFO) << "max key length " << key_counts_.size()-1;

        int64_t data_length = 0;
        for (size_t i = 0;i < key_counts_.size(); i++)
        {
            if (key_counts_[i] <= 0)
                continue;

            os.AppendInt32(i);

            if (option_.build_type == 0)
            {
                os.AppendInt64(data_length);
                data_length += data_lengths_[i];
            }
        }

        uint64_t pfd_length = 0;
        FileUtil::GetFileSize(pfd_file, &pfd_length);

        uint64_t trie_length = 0;
        FileUtil::GetFileSize(trie_file, &trie_length);
        auto index_offset = os.size() + sizeof(int32_t)*2 + sizeof(int64_t);
        os.AppendInt32(index_offset);
        os.AppendInt32(index_offset + pfd_length);
        os.AppendInt64(index_offset + pfd_length + trie_length);
    }
    
    std::string BuildPFD()
    {
        std::vector<uint64_t> v(keyset_.size());
        for (size_t i = 0;i < keyset_.size(); i++)
        {
            v[keyset_[i].id()] = offsets_[i];
        }

        auto name = option_.temp_folder + "pfd.dat";
        PForDelta pfd(v);
        pfd.Save(name);
        return name;
    }

    std::string BuildTrie()
    {
        marisa::Trie trie;
        trie.build(keyset_);

        std::string fname = option_.temp_folder + "trie.dat";
        trie.save(fname.c_str());
        return fname;
    }
   
    void MergeFiles(const std::vector<std::string>& files)
    {
        FileOutputStream os(fname_);
        for (auto& file : files)
        {
            if (!FileUtil::FileExists(file))
            {
                LOG(ERROR) << "Skip Merge " << file << " for it not exist";
                continue;
            }

            uint64_t size = 0;
            FileUtil::GetFileSize(file, &size);
            DLOG(INFO) << "Merging " << file << " size=" << size;

            FileUtil::SequentialFile tmp(file);
            char buf[8192];
            while (true)
            {
                StringPiece fragment;
                auto status = tmp.Read(sizeof buf, &fragment, buf);
                if (status)
                    break;

                if (fragment.empty())
                    break;
                os.Append(fragment);
            }
        }
    }
    
    void Cleanup(const std::vector<std::string>& files)
    {
        for (auto& file : files)
        {
            FileUtil::DeleteFile(file);
        }

        //FileUtil::DeleteDir(option_.temp_folder);
        //LOG(INFO) << "DeleteDir " << option_.temp_folder;
    }
    
    void ResizeData(size_t len)
    {
        if (key_counts_.size() <= len)
        {
            last_values_.resize(len+1, "");
            last_values_lengths_.resize(len+1, 0);

            data_lengths_.resize(len+1, 1);
            key_counts_.resize(len+1, 0);
        }
    }
    
    FileOutputStream* GetDataStream(size_t len)
    {
        if (data_streams_.size() <= len)
        {
            data_streams_.resize(len+1, NULL);
            data_files_.resize(len+1, "");
        }

        auto dos = data_streams_[len];
        if (!dos)
        {
            std::string file = option_.temp_folder + "data_" + std::to_string(len) + ".dat";
            data_files_[len] = file;
    
            dos = new FileOutputStream(file);
            data_streams_[len] = dos;
            
            dos->AppendInt8('\0');
        }

        return dos;
    }
    
    int32_t GetNumKeyCount() const
    {
        int32_t n = 0;
        for (auto& k : key_counts_)
        {
            if (k != 0)
            {
                n++;
            }
        }
        return n;
    }

    bool EqualLastValue(size_t len, const StringPiece& v) const
    {
        if (data_streams_.size() <= len || data_streams_[len] == NULL || key_counts_[len] == 0 || last_values_lengths_[len] != static_cast<int32_t>(v.length()))
        {
            return false;
        }

        return strncmp(v.data(), last_values_[len].data(), v.length()) == 0;
    }

private:
    Writer::Option option_;
    std::string fname_;
    bool done_;

    marisa::Keyset keyset_;

    std::vector<std::string> data_files_;
    std::vector<FileOutputStream*> data_streams_;

    std::vector<int64_t> data_lengths_;
    std::vector<int32_t> key_counts_;

    std::vector<std::string> last_values_;
    std::vector<int32_t> last_values_lengths_;

    std::vector<uint32_t> offsets_;
};

MarisaTrieWriter::MarisaTrieWriter(const Writer::Option& option, const std::string& fname)
    : impl_(new Impl(option, fname))
{
}

MarisaTrieWriter::~MarisaTrieWriter()
{
}

void MarisaTrieWriter::Put(const StringPiece& k)
{
    impl_->Put(k);
}

void MarisaTrieWriter::Put(const StringPiece& k, const StringPiece& v)
{
    impl_->Put(k, v);
}

void MarisaTrieWriter::Close()
{
    impl_->Close();
}

} // namespace
