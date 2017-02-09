#include "hash_writer.h"

#include <sys/mman.h>

#include <cmath>
#include <stdexcept>

#include <snappy.h>
#include <farmhash.h>
#include <glog/logging.h>

#include "utils/varint.h"
#include "utils/timestamp.h"
#include "utils/file_util.h"
#include "utils/file_stream.h"

namespace scdb {

const static char* kVersion = "SCDBV1.";

class HashWriter::Impl
{
public:
    Impl(const Writer::Option& option, const std::string& fname)
        : done_(false),
          option_(option),
          fname_(fname),
          num_keys_(0),
          num_values_(0),
          num_collisions_(0),
          indexes_length_(0)
    {
        if (option.compress_type == 2)
        {
            throw std::invalid_argument("Not Support Compress Type");
        }
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

        // each length has its own stream
        auto ios = GetIndexStream(len);

        // write key
        ios->Append(k);
    }

    void Put(const StringPiece& k, const StringPiece& v)
    {
        DCHECK(option_.build_type==0) << "Expect Build with value";

        auto len = k.length();
        if (len == 0)
            return ;

        // each length has its own stream
        auto ios = GetIndexStream(len);

        // write key
        ios->Append(k);

        bool same = EqualLastValue(len, v);

        // calcute offset
        int64_t data_length = data_lengths_[len];
        if (same)
        {
            data_length -= last_values_lengths_[len];
        }

        int offset_length = EncodeVarint(data_length, ios);
        max_offset_lengths_[len] = std::max(offset_length, max_offset_lengths_[len]);

        if (!same)
        {
            auto dos = GetDataStream(len);

            size_t value_length = v.length();
            size_t encode_length = 0;

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
            data_lengths_[len] += data_length + value_length;

            last_values_[len] = v.ToString();
            last_values_lengths_[len] = value_length + encode_length;

            num_values_++;
        }

        num_keys_++;
        key_counts_[len]++;
    }

    void Close()
    {
        if (done_)
            return ;

        for (size_t i = 0; i < index_streams_.size(); i++)
        {
            if (index_streams_[i])
                index_streams_[i]->Close();
        } 
    
        for (size_t i = 0; i < data_streams_.size(); i++)
        {
            if (data_streams_[i])
                data_streams_[i]->Close();
        }
    
        LOG(INFO) << "Number of keys: " << num_keys_;
    
        std::vector<std::string> files;

        std::string metadata_file = option_.temp_folder + "metadata.dat";
        WriteMetaData(metadata_file);
        files.push_back(metadata_file);

        for (size_t i = 0;i < index_files_.size(); i++)
        {
            if (!index_files_[i].empty())
            {
                files.push_back(BuildIndex(i));
            }
        }
    
        LOG(INFO) << "Number of collisions: " << num_collisions_;
    
        for (auto& file : data_files_)
        {
            if (!file.empty())
            {
                files.push_back(file);
            }
        }
    
        MergeFiles(files);
        Cleanup(files);

        done_ = true;
    }
    
    void WriteMetaData(const std::string& fname)
    {
        FileOutputStream os(fname);
    
        // WriteVersion
        os.Append(kVersion);
    
        // Write Time
        auto now = Timestamp::Now();
        os.AppendInt64(now.MicroSecondsSinceEpoch());

        // Write Option
        os.Append(reinterpret_cast<const int8_t*>(&option_.load_factor), sizeof(option_.load_factor));
        os.AppendInt8(option_.compress_type);
        os.AppendInt8(option_.build_type);
        os.AppendBool(option_.with_checksum);

        // Write Size
        os.AppendInt32(num_keys_);
        os.AppendInt32(GetNumKeyCount());
        os.AppendInt32(key_counts_.size()-1);
  
        int64_t data_length = 0; 
        for (size_t i = 0;i < key_counts_.size(); i++)
        {
            if (key_counts_[i] <= 0)
            {
                continue;
            }
    
            os.AppendInt32(i);
            os.AppendInt32(key_counts_[i]);
    
            int32_t slots = static_cast<int32_t>(round(key_counts_[i] / option_.load_factor));
            os.AppendInt32(slots);

            int offset_length = max_offset_lengths_[i];
            os.AppendInt32(i + offset_length);
    
            os.AppendInt32(indexes_length_);
            indexes_length_ += (i + offset_length) * slots;
   
            if (option_.build_type == 0)
            {
                os.AppendInt64(data_length);
                data_length += data_lengths_[i];
            }

        }
    
        int index_offset = os.size() + sizeof(int32_t);
        if (option_.build_type == 0)
            index_offset += sizeof(int64_t);

        os.AppendInt32(index_offset);
        os.AppendInt64(index_offset + indexes_length_);
    }
    
    std::string BuildIndex(size_t len)
    {
        int64_t count = key_counts_[len];
        int32_t slots = static_cast<int32_t>(round(count / option_.load_factor));
        int32_t offset_length = max_offset_lengths_[len];
        int32_t slot_size = len + offset_length;

        std::string fname = option_.temp_folder + "index_" + std::to_string(len) + ".dat";
        try
        {
            auto fd = ::open(fname.c_str(), O_RDWR | O_CREAT | O_TRUNC);
            CHECK(fd) << "Create " << fname << " failed";

            auto addr = reinterpret_cast<char*>(::mmap(NULL, slot_size*slots, PROT_WRITE, MAP_PRIVATE, fd, 0));
            try
            {
                FileInputStream is(index_files_[len]);
                std::vector<char> key_buf(len);
                std::vector<char> slot_buf(slot_size);
                std::vector<char> offset_buf(offset_length);
                for (int i = 0;i < count; i++)
                {
                    is.Read(key_buf);
                    int64_t offset = static_cast<int64_t>(DecodeVarint(is));

                    uint64_t hash = util::Hash64(std::string(&key_buf[0], len));

                    bool collision = false;
                    for (int64_t probe = 0;probe < slots; probe++)
                    {
                        auto slot = static_cast<int32_t>((hash + probe) % slots);
                        char* pos = addr+slot*slot_size;
                        memcpy(&slot_buf[0], pos, slot_size);

                        int64_t found = static_cast<int64_t>(DecodeVarint(slot_buf, 0, NULL));
                        if (found == 0)
                        {
                            // slot empty
                            memcpy(pos, &key_buf[0], key_buf.size());
                            auto n = EncodeVarint(offset, offset_buf);
                            memcpy(pos+key_buf.size(), &offset_buf[0], n);
                            break;
                        }
                        else
                        {
                            collision = true;
                            if (strncmp(&key_buf[0], &slot_buf[0], len) == 0)
                            {
                                LOG(ERROR) << "Found same key " << std::string(key_buf.data(), key_buf.size());
                            }
                        }
                    }

                    if (collision)
                        num_collisions_++;
                }
            }
            catch (const std::exception& ex)
            {
                LOG(ERROR) << "BuildIndex failed: " << ex.what();
            }

            ::munmap(addr, slot_size*slots);
            ::close(fd);

            FileUtil::DeleteFile(index_files_[len]);
            LOG(INFO) << "DeleteFile " << index_files_[len];
        }
        catch (...)
        {
        }

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
            LOG(INFO) << "Merging " << file << " size=" << size;

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
            LOG(INFO) << "DeleteFile " << file;
        }

        //FileUtil::DeleteDir(option_.temp_folder);
        //LOG(INFO) << "DeleteDir " << option_.temp_folder;
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
    
    FileOutputStream* GetIndexStream(size_t len)
    {
        if (index_streams_.size() <= len)
        {
            index_streams_.resize(len+1, NULL);
            index_files_.resize(len+1, "");

            key_counts_.resize(len+1, 0);
            max_offset_lengths_.resize(len+1, 0);

            data_lengths_.resize(len+1, 0);

            last_values_.resize(len+1, "");
            last_values_lengths_.resize(len+1, 0);
        }
    
        auto ios = index_streams_[len];
        if (!ios)
        {
            auto file = option_.temp_folder + "temp_index_" + std::to_string(len) + ".dat";
            index_files_[len] = file;
    
            ios = new FileOutputStream(file);
            index_streams_[len] = ios;
    
            data_lengths_[len]++;
        }
    
        return ios;
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
        if (index_streams_[len] == NULL || key_counts_[len] == 0 || last_values_lengths_[len] != static_cast<int32_t>(v.length()))
        {
            return false;
        }

        return strncmp(v.data(), last_values_[len].data(), v.length()) == 0;
    }

private:
    bool done_;

    Option option_;
    std::string fname_;

    int32_t num_keys_;
    int32_t num_values_;
    int32_t num_collisions_;

    std::vector<std::string> index_files_;
    std::vector<FileOutputStream*> index_streams_;

    std::vector<std::string> data_files_;
    std::vector<FileOutputStream*> data_streams_;

    std::vector<int64_t> data_lengths_;
    int64_t indexes_length_;
    
    std::vector<int32_t> key_counts_;
    std::vector<int32_t> max_offset_lengths_;

    std::vector<std::string> last_values_;
    std::vector<int32_t> last_values_lengths_;
};

HashWriter::HashWriter(const Writer::Option& option, const std::string& fname)
    : impl_(new Impl(option, fname))
{
}

HashWriter::~HashWriter()
{
}

void HashWriter::Put(const StringPiece& k)
{
    impl_->Put(k);
}

void HashWriter::Put(const StringPiece& k, const StringPiece& v)
{
    impl_->Put(k, v);
}

void HashWriter::Close()
{
    impl_->Close();
}

} // namespace
