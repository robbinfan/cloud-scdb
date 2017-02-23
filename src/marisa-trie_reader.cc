#include "marisa-trie_reader.h"

#include <unistd.h>
#include <sys/mman.h>

#include <exception>

#include "marisa/trie.h"
#include <snappy.h>
#include <glog/logging.h>

#include "scdb/writer.h"

#include "utils/varint.h"
#include "utils/pfordelta.h"
#include "utils/timestamp.h"
#include "utils/file_stream.h"
#include "utils/file_util.h"

namespace scdb {

class MarisaTrieReader::Impl
{
public:
    Impl(const Reader::Option& option, const std::string& fname)
        : option_(option),
          get_func_(&Impl::GetEmpty),
          get_as_string_func_(&Impl::GetEmptyAsString),
          get_as_string_by_id_func_(&Impl::GetEmptyAsStringById)

    {
        int32_t pfd_offset = 0;
        int32_t key_trie_offset = 0;
        int64_t data_offset = 0;
        try
        {
            FileInputStream is(fname); 
            char buf[7];
    
            is.Read(buf, sizeof buf);
            CHECK(strncmp(buf, "SCDBV1.", sizeof buf) == 0) << "Invalid Format: miss match format";
    
            is.Read<int64_t>(); // Timestamp
    
            // Writer Option
            writer_option_.compress_type = static_cast<Writer::CompressType>(is.Read<int8_t>());
            writer_option_.build_type = static_cast<Writer::BuildType>(is.Read<int8_t>());
            writer_option_.with_checksum = is.Read<bool>();

            if (writer_option_.build_type == Writer::kMap && writer_option_.compress_type != Writer::kDFA)
            {
                auto num_key_length = is.Read<int32_t>();
                auto max_key_length = is.Read<int32_t>();
    
                DLOG(INFO) << "num key count " << num_key_length;
                DLOG(INFO) << "max key length " << max_key_length;
   
                data_offsets_.resize(max_key_length+1, 0);

                for (int32_t i = 0;i < num_key_length; i++)
                {
                    auto len = is.Read<int32_t>();
                    data_offsets_[len] = is.Read<int64_t>();
                }
            }
    
            pfd_offset = is.Read<int32_t>();
            key_trie_offset = is.Read<int32_t>();
            data_offset = is.Read<int64_t>();

            // Must Load pfd first
            if (writer_option_.build_type == Writer::kMap)
            {
                pfd_.Load(fname, pfd_offset);
            }
        }
        catch (const std::exception& ex)
        {
            LOG(ERROR) << "MarisaTrieReader ctor failed " << ex.what();
            throw;
        }
 
        if (writer_option_.with_checksum && !FileUtil::IsValidCheckedFile(fname))
        {
            throw std::runtime_error("verify checksum failed: " + fname);
        }

        fd_ = ::open(fname.c_str(), O_RDONLY);
        CHECK(fd_) << "open " << fname << " failed";
        FileUtil::GetFileSize(fname, &length_);

        auto page_size = sysconf(_SC_PAGE_SIZE);
        auto offset = (key_trie_offset / page_size) * page_size;
        auto page_offset = key_trie_offset % page_size;

        auto mflag = MAP_SHARED;
        if (option_.mmap_preload)
            mflag |= MAP_POPULATE;
        auto mptr = ::mmap(NULL, length_, PROT_READ, mflag, fd_, offset);
        CHECK (mptr != MAP_FAILED) << "mmap failed " << fname;
        ptr_ = reinterpret_cast<char*>(mptr);

        index_ptr_ = ptr_ + page_offset;
        if (writer_option_.build_type == Writer::kMap)
        {
            data_ptr_ =  ptr_ + page_offset  + (data_offset - key_trie_offset);
        }
        key_trie_.map(index_ptr_, data_offset - key_trie_offset);

        if (writer_option_.compress_type == Writer::kDFA)
        {
            value_trie_.map(data_ptr_, length_ - sizeof(uint32_t) - data_offset);
        }

        if (writer_option_.build_type == Writer::kMap)
        {
            switch (writer_option_.compress_type)
            {
                case Writer::kNone:
                    get_func_ = &Impl::GetRawValue;
                    get_as_string_func_ = &Impl::GetRawValueAsString;
                    get_as_string_by_id_func_ = &Impl::GetRawValueAsStringById;
                    break;
                case Writer::kSnappy:
                    get_as_string_func_ = &Impl::GetCompressedValueAsString;
                    get_as_string_by_id_func_ = &Impl::GetCompressedValueAsStringById;
                    break;
                case Writer::kDFA:
                    get_as_string_func_ = &Impl::GetDFAValue;
                    get_as_string_by_id_func_ = &Impl::GetDFAValueById;
                    break;
            }
        }
    }
    
    ~Impl()
    {
        ::munmap(ptr_, length_);
        ::close(fd_);
    }
 
    std::vector<std::pair<std::string, std::string>> PrefixGet(const StringPiece& k, size_t count) const
    {
        std::vector<std::pair<std::string, std::string>> m;

        marisa::Agent agent;
        agent.set_query(k.data(), k.length());
        marisa::Keyset keys;
        try
        {
            while (key_trie_.predictive_search(agent))
            {
                keys.push_back(agent.key());
            }

            auto end = std::min(count, keys.size());
            for (size_t i = 0;i < end; i++)
            {
                m.push_back(std::make_pair(std::string(keys[i].ptr(), keys[i].length()),
                                           GetAsStringById(keys[i].id(), keys[i].length())));
            }
        }
        catch (const marisa::Exception &ex)
        {
            LOG(ERROR) << ex.what() << ": PrefixGet() failed: "
                       << k.ToString();
        }

        return m;
    }

    StringPiece GetRawValue(const StringPiece& k) const
    {
        StringPiece result("");
        if (writer_option_.build_type == Writer::kSet)
        {
            return result;
        }

        marisa::Agent agent;
        agent.set_query(k.data(), k.length());
        if (!key_trie_.lookup(agent))
        {
            return result;
        }
 
        return GetRawValueById(agent.key().id(), k.length());
    }

    StringPiece GetRawValueById(uint32_t id, size_t len) const
    {
        auto offset = pfd_.Extract(id);
        auto data_offset = data_offsets_[len];
        auto block_ptr = reinterpret_cast<const int8_t*>(data_ptr_ + data_offset + offset);

        size_t prefix_length;
        auto value_length = DecodeVarint(block_ptr, block_ptr + 10, &prefix_length);
        return StringPiece(reinterpret_cast<const char*>(block_ptr + prefix_length), value_length);
    }

    std::string GetRawValueAsString(const StringPiece& k) const
    {
        return GetRawValue(k).ToString();
    }

    std::string GetRawValueAsStringById(uint32_t id, size_t len) const
    {
        return GetRawValueById(id, len).ToString();
    }

    StringPiece GetEmpty(const StringPiece& key) const
    {
        return StringPiece("");
    }

    std::string GetEmptyAsString(const StringPiece& key) const
    {
        return "";
    }

    std::string GetEmptyAsStringById(uint32_t id, size_t len) const
    {
        return "";
    }

    std::string GetDFAValueById(uint32_t id, size_t) const
    {
        auto idx = pfd_.Extract(id);
        marisa::Agent agent;
        agent.set_query(idx);
        std::string result;
        key_trie_.reverse_lookup(agent);
        return result.assign(agent.key().ptr(), agent.key().length());
    }

    std::string GetDFAValue(const StringPiece& key) const
    {
        marisa::Agent agent;
        agent.set_query(key.data(), key.length());
        if (!key_trie_.lookup(agent))
        {
            return "";
        }
        return GetDFAValueById(agent.key().id(), 0);
    }

    std::string GetCompressedValueAsString(const StringPiece& key) const
    {
        auto v = GetRawValue(key);
        std::string ucv;
        snappy::Uncompress(v.data(), v.length(), &ucv);
        return ucv;
    }

    std::string GetCompressedValueAsStringById(uint32_t id, size_t len) const
    {
        auto v = GetRawValueById(id, len);
        std::string ucv;
        snappy::Uncompress(v.data(), v.length(), &ucv);
        return ucv;
    }

    bool Exist(const StringPiece& key) const
    {
        marisa::Agent agent;
        agent.set_query(key.data(), key.length());
        return key_trie_.lookup(agent);
    }

    StringPiece Get(const StringPiece& key) const
    {
        return (this->*get_func_)(key);
    }

    std::string GetAsString(const StringPiece& key) const
    {
        return (this->*get_as_string_func_)(key);
    }

    std::string GetAsStringById(uint32_t id, size_t len) const
    {
        return (this->*get_as_string_by_id_func_)(id, len);
    }

private:
    Reader::Option option_;
    Writer::Option writer_option_;

    typedef StringPiece (Impl::*GetFunc)(const StringPiece&) const;
    typedef std::string (Impl::*GetAsStringFunc)(const StringPiece&) const;
    typedef std::string (Impl::*GetAsStringByIdFunc)(uint32_t id, size_t len) const;
    typedef bool (Impl::*ExistFunc)(const StringPiece&) const;

    int fd_;
    uint64_t length_;
    char* ptr_;

    std::vector<int64_t> data_offsets_;

    const char* index_ptr_;
    const char* data_ptr_;

    marisa::Trie key_trie_;
    marisa::Trie value_trie_;
    PForDelta pfd_;

    GetFunc get_func_;
    GetAsStringFunc get_as_string_func_;
    GetAsStringByIdFunc get_as_string_by_id_func_;
}; 

MarisaTrieReader::MarisaTrieReader(const Reader::Option& option, const std::string& fname)
    : impl_(new Impl(option, fname))
{
}

MarisaTrieReader::~MarisaTrieReader()
{
}

bool MarisaTrieReader::Exist(const StringPiece& k) const
{
    return impl_->Exist(k);
}

StringPiece MarisaTrieReader::Get(const StringPiece& k) const
{
    return impl_->Get(k);
}

std::string MarisaTrieReader::GetAsString(const StringPiece& k) const
{
    return impl_->GetAsString(k);
}

std::vector<std::pair<std::string, std::string>> MarisaTrieReader::PrefixGet(const StringPiece& prefix, size_t count) const
{
    return impl_->PrefixGet(prefix, count);
}

} // namespace
