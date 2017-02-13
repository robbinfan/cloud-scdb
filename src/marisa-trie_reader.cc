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
          exist_func_(&Impl::ExistRawKey)

    {
        int32_t pfd_offset = 0;
        int32_t trie_offset = 0;
        int64_t data_offset = 0;
        try
        {
            (void)option_;

            FileInputStream is(fname); 
            char buf[7];
    
            is.Read(buf, sizeof buf);
            CHECK(strncmp(buf, "SCDBV1.", sizeof buf) == 0) << "Invalid Format: miss match format";
    
            is.Read<int64_t>(); // Timestamp
    
            // Writer Option
            writer_option_.compress_type = is.Read<int8_t>();
            writer_option_.build_type = is.Read<int8_t>();
            writer_option_.with_checksum = is.Read<bool>();

            if (!writer_option_.IsNoDataSection())
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
            trie_offset = is.Read<int32_t>();
            data_offset = is.Read<int64_t>();

            // Must Load pfd first
            if (!writer_option_.IsNoDataSection())
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
            throw std::invalid_argument("verify checksum failed: " + fname);
        }

        fd_ = ::open(fname.c_str(), O_RDONLY);
        CHECK(fd_) << "open " << fname << " failed";

        FileUtil::GetFileSize(fname, &length_);

        auto page_size = sysconf(_SC_PAGE_SIZE);
        auto offset = (trie_offset / page_size) * page_size;
        auto page_offset = trie_offset % page_size;

        auto mflag = MAP_SHARED;
        if (option_.mmap_preload)
            mflag |= MAP_POPULATE;
        auto mptr = ::mmap(NULL, length_, PROT_READ, mflag, fd_, offset);
        CHECK (mptr != MAP_FAILED) << "mmap failed " << fname;
        ptr_ = reinterpret_cast<char*>(mptr);

        index_ptr_ = ptr_ + page_offset;

        if (!writer_option_.IsNoDataSection())
        {
            data_ptr_ =  ptr_ + page_offset  + (data_offset - trie_offset);
        }
    
        trie_.map(index_ptr_, data_offset - trie_offset);

        if (writer_option_.build_type == 0)
        {
            switch (writer_option_.compress_type)
            {
                case 0:
                    get_func_ = &Impl::GetRawKey;
                    get_as_string_func_ = &Impl::GetRawKeyAsString;
                    break;
                case 1:
                    get_as_string_func_ = &Impl::GetCompressedValueAsString;
                    break;
                case 2:
                    get_as_string_func_ = &Impl::GetPrefixKeyAsString;
                    break;
            }
        }

        if (writer_option_.compress_type == 2)
        {
            exist_func_ = &Impl::ExistPrefixKey;
        }
    }
    
    ~Impl()
    {
        ::munmap(ptr_, length_);
        ::close(fd_);
    }
  
    std::vector<std::pair<std::string, std::string>> PrefixGetAsString(const StringPiece& k, size_t count) const
    {
        std::vector<std::pair<std::string, std::string>> m;

        marisa::Agent agent;
        agent.set_query(k.data(), k.length());

        marisa::Keyset keyset;
        try
        {
            while (trie_.predictive_search(agent))
            {
                keyset.push_back(agent.key());
            }

            auto end = std::min(count, keyset.size());
            for (size_t i = 0;i < end; i++)
            {
                StringPiece tk(keyset[i].ptr(), keyset[i].length());
                if (writer_option_.build_type == 0)
                {
                    if (writer_option_.compress_type == 2)
                    {
                        auto pos = tk.find('\t', k.length());
                        if (pos == StringPiece::npos)
                            continue;

                        m.push_back(std::make_pair(tk.substr(0, k.length()+pos).ToString(),
                                                   tk.substr(k.length()+pos+1).ToString()));

                    }
                    else
                    {
                        m.push_back(std::make_pair(tk.ToString(), GetAsString(tk)));
                    }
                }
                else
                {
                    m.push_back(std::make_pair(tk.ToString(), ""));
                }
            }
        }
        catch (const marisa::Exception &ex)
        {
            LOG(ERROR) << ex.what() << ": PrefixGet() failed: "
                       << k.ToString();
        }

        return m;
    }

    std::vector<std::pair<std::string, StringPiece>> PrefixGet(const StringPiece& k, size_t count) const
    {
        std::vector<std::pair<std::string, StringPiece>> m;

        marisa::Agent agent;
        agent.set_query(k.data(), k.length());

        marisa::Keyset keyset;
        try
        {
            while (trie_.predictive_search(agent))
            {
                keyset.push_back(agent.key());
            }

            auto end = std::min(count, keyset.size());
            for (size_t i = 0;i < end; i++)
            {
                m.push_back(std::make_pair(std::string(keyset[i].ptr(), keyset[i].length()),
                                           GetValue(keyset[i].id(), keyset[i].length())));
                if (writer_option_.build_type == 0 && writer_option_.compress_type == 0)
                {
                    m.push_back(std::make_pair(std::string(keyset[i].ptr(), keyset[i].length()),
                                               GetValue(keyset[i].id(), keyset[i].length())));
                }
                else
                {
                    m.push_back(std::make_pair(std::string(keyset[i].ptr(), keyset[i].length()), StringPiece("")));
                }
            }
        }
        catch (const marisa::Exception &ex)
        {
            LOG(ERROR) << ex.what() << ": PrefixGet() failed: "
                       << k.ToString();
        }

        return m;
    }

    StringPiece GetValue(uint32_t id, size_t len) const
    {
        if (writer_option_.IsNoDataSection())
        {
            return StringPiece("");
        }

        auto offset = pfd_.Extract(id);
        auto data_offset = data_offsets_[len];
        auto block_ptr = reinterpret_cast<const int8_t*>(data_ptr_ + data_offset + offset);

        size_t prefix_length;
        auto value_length = DecodeVarint(block_ptr, block_ptr + 10, &prefix_length);
        return StringPiece(reinterpret_cast<const char*>(block_ptr + prefix_length), value_length);
    }

    StringPiece GetRawKey(const StringPiece& k) const
    {
        DCHECK(!writer_option_.IsNoDataSection()) << "Invalid Operation, No Value has been load!!!";

        StringPiece result("");
        marisa::Agent agent;
        agent.set_query(k.data(), k.length());
        if (!trie_.lookup(agent))
        {
            return result;
        }
  
        return GetValue(agent.key().id(), k.length());
    }

    std::string GetRawKeyAsString(const StringPiece& k) const
    {
        return GetRawKey(k).ToString();
    }


    StringPiece GetEmpty(const StringPiece& key) const
    {
        return StringPiece("");
    }

    std::string GetEmptyAsString(const StringPiece& key) const
    {
        return "";
    }

    std::string GetPrefixKeyAsString(const StringPiece& key) const
    {
        std::string result;

        marisa::Agent agent;
        agent.set_query(key.data(), key.length());

        while (trie_.predictive_search(agent))
        {
            if (agent.key()[key.length()] == '\t')
            {
                result.assign(agent.key().ptr()+key.length()+1, agent.key().length()-key.length()-1);
                break;
            }
        }
        return result;
    }

    std::string GetCompressedValueAsString(const StringPiece& key) const
    {
        auto v = GetRawKey(key);

        std::string ucv;
        snappy::Uncompress(v.data(), v.length(), &ucv);
        return ucv;
    }

    bool ExistRawKey(const StringPiece& key) const
    {
        marisa::Agent agent;
        agent.set_query(key.data(), key.length());
        return trie_.lookup(agent);
    }

    bool ExistPrefixKey(const StringPiece& key) const
    {
        marisa::Agent agent;
        agent.set_query(key.data(), key.length());
        if ((trie_.predictive_search(agent))
            && (key.length() == agent.key().length()))
        {
            return true;
        }
        return false;
    }

    bool Exist(const StringPiece& key) const
    {
        return (this->*exist_func_)(key);
    }

    StringPiece Get(const StringPiece& key) const
    {
        return (this->*get_func_)(key);
    }

    std::string GetAsString(const StringPiece& key) const
    {
        return (this->*get_as_string_func_)(key);
    }

private:
    Reader::Option option_;
    Writer::Option writer_option_;

    typedef StringPiece (Impl::*GetFunc)(const StringPiece&) const;
    typedef std::string (Impl::*GetAsStringFunc)(const StringPiece&) const;
    typedef bool (Impl::*ExistFunc)(const StringPiece&) const;

    int fd_;
    uint64_t length_;
    char* ptr_;

    std::vector<int64_t> data_offsets_;

    const char* index_ptr_;
    const char* data_ptr_;

    marisa::Trie trie_;
    PForDelta pfd_;

    GetFunc get_func_;
    GetAsStringFunc get_as_string_func_;
    ExistFunc exist_func_;
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

std::vector<std::pair<std::string, StringPiece>> MarisaTrieReader::PrefixGet(const StringPiece& prefix, size_t count) const
{
    return impl_->PrefixGet(prefix, count);
}

std::vector<std::pair<std::string, std::string>> MarisaTrieReader::PrefixGetAsString(const StringPiece& prefix, size_t count) const
{
    return impl_->PrefixGetAsString(prefix, count);
}

} // namespace
