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

const static char* kVersion = "SCDBV1.";

class MarisaTrieWriter::Impl
{
public:
    Impl(const Writer::Option& option, const std::string& fname)
        : option_(option),
          fname_(fname),
          closed_(false)
    {
        if (option_.build_type == kMap)
        {
            if (option_.compress_type == kDFA)
            {
                put_func_ = &Impl::PutAsTrie;
            }
            else
            {
                put_func_ = &Impl::PutRawOrSnappy;
            }
        }
    }

    ~Impl()
    {
        Close();
    }

    void Put(const StringPiece& k)
    {
        DCHECK(option_.build_type==kSet) << "Expect Build without value";

        auto len = k.length();
        if (len == 0)
            return ;

        marisa::Key key;
        key.set_str(k.data(), len);
        keys_.push_back(key);
    }

    void Put(const StringPiece& k, const StringPiece& v)
    {
        if (k.length() == 0) // unlikely
            return ;
        (this->*put_func_)(k, v);
    }

    void PutAsTrie(const StringPiece& k, const StringPiece& v)
    {
        marisa::Key key;
        key.set_str(k.data(), k.length());
        keys_.push_back(key);

        marisa::Key value;
        value.set_str(v.data(), v.length());
        values_.push_back(value);
    }

    void PutRawOrSnappy(const StringPiece& k, const StringPiece& v)
    {
        DCHECK(!option_.IsNoDataSection()) << "Expect Build with value";

        auto len = k.length();
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
            if (option_.compress_type == kSnappy)
            {
                std::string cv;
                snappy::Compress(v.data(), v.length(), &cv);
                value_length = cv.length();

                encode_length = EncodeVarint(value_length, dos);
                dos->Append(cv);
            }
            else
            {
                encode_length = EncodeVarint(v.length(), dos);
                dos->Append(v);
            }

            data_lengths_[len] += encode_length + value_length;

            last_values_[len] = v.ToString();
            last_values_lengths_[len] = value_length + encode_length;
        }

        marisa::Key key;
        key.set_str(k.data(), len);
        keys_.push_back(key);
        offsets_.push_back(data_length);
        key_counts_[len]++;
    }

    void Close()
    {
        if (closed_)
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
        auto key_trie_file = BuildTrie(keys_, "key_trie"); // Must build trie first
        std::string value_trie_file;
        if (option_.compress_type == kDFA)
        {
            value_trie_file = BuildTrie(values_, "value_trie");
        }

        auto pfd_file = BuildPFD();
        std::string metadata_file = option_.temp_folder + "metadata.dat";
        WriteMetaData(metadata_file, pfd_file, key_trie_file);

        files.push_back(metadata_file);
        if (!pfd_file.empty())
            files.push_back(pfd_file);
        files.push_back(key_trie_file); // let trie closed to data, they will mmape together
        if (!value_trie_file.empty())
            files.push_back(value_trie_file);

        for (auto& file : data_files_)
        {
            if (!file.empty())
            {
                files.push_back(file);
            }
        }
    
        MergeFiles(files);
        if (option_.with_checksum)
        {
            FileUtil::AddChecksumToFile(fname_);
        }

        Cleanup(files);
        closed_ = true;
    }
    
    void WriteMetaData(const std::string& fname, 
                       const std::string& pfd_file, 
                       const std::string& key_trie_file)
    {
        FileOutputStream os(fname);
    
        // WriteVersion
        os.Append(kVersion);
    
        // Write Time
        auto now = Timestamp::Now();
        os.Append(now.MicroSecondsSinceEpoch());

        // Write Option
        os.Append<int8_t>(option_.compress_type);
        os.Append<int8_t>(option_.build_type);
        os.Append(option_.with_checksum);

        if (!option_.IsNoDataSection() && option_.compress_type != kDFA)
        {
            os.Append<int32_t>(GetNumKeyCount());
            os.Append<int32_t>(key_counts_.size()-1);

            DLOG(INFO) << "num key count " << GetNumKeyCount();
            DLOG(INFO) << "max key length " << key_counts_.size()-1;

            int64_t data_length = 0;
            for (size_t i = 0;i < key_counts_.size(); i++)
            {
                if (key_counts_[i] <= 0)
                    continue;
                os.Append<int32_t>(i);

                os.Append<int64_t>(data_length);
                data_length += data_lengths_[i];
            }
        }

        uint64_t pfd_length = 0;
        if (!pfd_file.empty())
            FileUtil::GetFileSize(pfd_file, &pfd_length);

        uint64_t key_trie_length = 0;
        FileUtil::GetFileSize(key_trie_file, &key_trie_length);

        auto index_offset = os.size() + sizeof(int32_t)*2 + sizeof(int64_t);
        os.Append<int32_t>(index_offset);
        os.Append<int32_t>(index_offset + pfd_length);
        os.Append<int64_t>(index_offset + pfd_length + key_trie_length);
    }
    
    std::string BuildPFD()
    {
        if (option_.IsNoDataSection())
            return "";

        std::vector<uint64_t> v(keys_.size());
        if (option_.compress_type == kDFA)
        {
            for (size_t i = 0;i < keys_.size(); i++)
            {
                v[keys_[i].id()] = values_[i].id();
            }
        }
        else
        {
            for (size_t i = 0;i < keys_.size(); i++)
            {
                v[keys_[i].id()] = offsets_[i];
            }
        }

        auto name = option_.temp_folder + "pfd.dat";
        PForDelta pfd(v);
        pfd.Save(name);
        return name;
    }

    std::string BuildTrie(marisa::Keyset& s, const std::string& prefix)
    {
        marisa::Trie trie;
        trie.build(s);

        std::string fname = option_.temp_folder + prefix + ".dat";
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
            
            dos->Append('\0');
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
    bool closed_;

    marisa::Keyset keys_;
    marisa::Keyset values_;

    std::vector<std::string> data_files_;
    std::vector<FileOutputStream*> data_streams_;

    std::vector<int64_t> data_lengths_;
    std::vector<int32_t> key_counts_;

    std::vector<std::string> last_values_;
    std::vector<int32_t> last_values_lengths_;

    std::vector<uint32_t> offsets_;

    typedef void (Impl::*PutFunc)(const StringPiece&, const StringPiece&);
    PutFunc put_func_;
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
