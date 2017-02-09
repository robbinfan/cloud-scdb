#include "hash_reader.h"

#include <sys/mman.h>

#include <snappy.h>
#include <farmhash.h>
#include <glog/logging.h>

#include "scdb/writer.h"

#include "utils/varint.h"
#include "utils/timestamp.h"
#include "utils/file_stream.h"

namespace scdb {

class HashReader::Impl
{
public:
    Impl(const Reader::Option& option, const std::string& fname)
        : option_(option),
          fd_(-1),
          length_(0),
          ptr_(NULL),
          index_offset_(-1),
          data_offset_(-1),
          index_ptr_(NULL),
          data_ptr_(NULL)
    {
        try
        {
            FileInputStream is(fname); 
            char buf[7];
    
            is.Read(buf, sizeof buf);
            CHECK(strncmp(buf, "SCDBV1.", 7) == 0) << "Invalid Format: miss match format";

            is.ReadInt64(); // create at
   
            // Writer Option
            is.Read(reinterpret_cast<char*>(&writer_option_.load_factor), sizeof(writer_option_.load_factor));
            writer_option_.compress_type = is.ReadInt8();
            writer_option_.build_type = is.ReadInt8();
            writer_option_.with_checksum = is.ReadBool();

            auto num_keys = is.ReadInt32();
            auto num_key_length = is.ReadInt32();
            auto max_key_length = is.ReadInt32();
    
            LOG(INFO) << "num keys " << num_keys;
            LOG(INFO) << "num key count " << num_key_length;
            LOG(INFO) << "max key length " << max_key_length;
    
            index_offsets_.resize(max_key_length+1, 0);
            key_counts_.resize(max_key_length+1, 0);
            slots_.resize(max_key_length+1, 0);
            slots_size_.resize(max_key_length+1, 0);
            if (writer_option_.build_type == 0)
            {
                data_offsets_.resize(max_key_length+1, 0);
            }
            
            int max_slot_size = 0;
            for (int32_t i = 0;i < num_key_length; i++)
            {
                auto len = is.ReadInt32();
    
                key_counts_[len] = is.ReadInt32();
                slots_[len] = is.ReadInt32();
                slots_size_[len] = is.ReadInt32();
                index_offsets_[len] = is.ReadInt32();

                if (writer_option_.build_type == 0)
                {
                    data_offsets_[len] = is.ReadInt64();
                }
                
                max_slot_size = std::max(max_slot_size, slots_size_[len]);
            }
            slot_buf_.resize(max_slot_size);
    
            index_offset_ = is.ReadInt32();
            if (writer_option_.build_type == 0)
            {
                data_offset_ = is.ReadInt64();
            }
        }
        catch (const std::exception& ex)
        {
            LOG(ERROR) << "HashReader ctor failed: " << ex.what();
            throw;
        }
    
        fd_ = ::open(fname.c_str(), O_RDONLY);
        CHECK(fd_) << "open " << fname << " failed";

        FileUtil::GetFileSize(fname, &length_);
        auto ptr_ = reinterpret_cast<char *>(::mmap(NULL, length_, PROT_READ, MAP_SHARED, fd_, 0));
        //DCHECK(reinterpret_cast<int>(ptr_) != -1) << "mmap " << fname << " failed";

        index_ptr_ = ptr_ + index_offset_;
        if (writer_option_.build_type == 0)
        {
            data_ptr_ =  ptr_ + data_offset_;
        }
    }
   
    ~Impl()
    {
        ::munmap(ptr_, length_);
        ::close(fd_);
    }
    
    StringPiece GetInternal(const StringPiece& k) const
    {
        DCHECK(writer_option_.build_type == 0) << "Invalid Operation, No Value has been load!!!";

        StringPiece result("");
        if (writer_option_.build_type != 0)
        {
            return result;
        }
        
        auto len = k.length();
        if (len > slots_.size() || key_counts_[len] == 0)
        {
            return result;        
        }    
    
        auto hash = util::Hash64(k.data(), k.length());
        auto num_slots = slots_[len];
        auto slot_size = slots_size_[len];
        auto index_offset = index_offsets_[len];
        auto data_offset = data_offsets_[len];
    
        for (int32_t probe = 0;probe < num_slots; probe++)
        {
            auto slot = static_cast<int32_t>((hash + probe) % num_slots);
            auto pos = index_ptr_ + index_offset + slot*slot_size;
            memcpy(const_cast<char*>(&slot_buf_[0]), pos, slot_size);
    
            if (strncmp(&slot_buf_[0], k.data(), len))
                continue;
    
            size_t offset_length = 0;
            auto offset = DecodeVarint(slot_buf_, len, &offset_length);
            if (offset == 0)
            {
                return result;
            }
    
            size_t prefix_length = 0;
            auto block_ptr = reinterpret_cast<const int8_t*>(data_ptr_ + data_offset + offset);
            auto value_length = DecodeVarint(block_ptr, block_ptr + 10, &prefix_length);
            result.reset(reinterpret_cast<const char*>(block_ptr + prefix_length), value_length);
            break;
        }
    
        return result;
    }

    StringPiece Get(const StringPiece& k) const
    {
        DCHECK(!writer_option_.compress_type==0) << "API Not Compressed Value, Use GetAsString() Instread!!!";
        return GetInternal(k);
    }

    std::string GetAsString(const StringPiece& k) const
    {
        DCHECK(!writer_option_.compress_type==1) << "API Expect only use when value compressed!!!";

        auto cv = GetInternal(k);
        if (writer_option_.compress_type == 0)
        {
            return cv.ToString();
        }

        std::string ucv;
        snappy::Uncompress(k.data(), k.length(), &ucv);
        return ucv;
    }

    bool Exist(const StringPiece& k) const
    {
        auto len = k.length();
        if (len > slots_.size() || key_counts_[len] == 0)
        {
            return false;
        }    
    
        auto hash = util::Hash64(k.data(), k.length());
        auto num_slots = slots_[len];
        auto slot_size = slots_size_[len];
        auto index_offset = index_offsets_[len];
        for (int32_t probe = 0;probe < num_slots; probe++)
        {
            auto slot = static_cast<int32_t>((hash + probe) % num_slots);
            auto pos = index_ptr_ + index_offset + slot*slot_size;
            memcpy(const_cast<char*>(&slot_buf_[0]), pos, slot_size);
    
            if (strncmp(&slot_buf_[0], k.data(), len) == 0)
            {
                return true;
            }
        }
    
        return false;
    }

private:
    Reader::Option option_;
    Writer::Option writer_option_;

    int fd_;
    size_t length_;
    char* ptr_;

    std::vector<int32_t> index_offsets_;
    std::vector<int64_t> data_offsets_;
    std::vector<int32_t> key_counts_;
    std::vector<int32_t> slots_;
    std::vector<int32_t> slots_size_;
    mutable std::vector<char> slot_buf_;

    int32_t index_offset_;
    int64_t data_offset_;

    const char* index_ptr_;
    const char* data_ptr_;
};

HashReader::HashReader(const Reader::Option& option, const std::string& fname)
    : impl_(new Impl(option, fname))
{
}

HashReader::~HashReader()
{
}

bool HashReader::Exist(const StringPiece& k) const
{
    return impl_->Exist(k);
}

StringPiece HashReader::Get(const StringPiece& k) const
{
    return impl_->Get(k);
}

std::string HashReader::GetAsString(const StringPiece& k) const
{
    return impl_->GetAsString(k);
}

} // namespace
