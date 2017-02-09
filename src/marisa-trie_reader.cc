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
        : option_(option)
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
            CHECK(strncmp(buf, "SCDBV2.", sizeof buf) == 0) << "Invalid Format: miss match format";
    
            is.ReadInt64(); // Timestamp
    
            // Writer Option
            is.Read(reinterpret_cast<char*>(&writer_option_.load_factor), sizeof(writer_option_.load_factor));
            writer_option_.compress_type = is.ReadInt8();
            writer_option_.build_type = is.ReadInt8();
            writer_option_.with_checksum = is.ReadBool();

            auto num_key_length = is.ReadInt32();
            auto max_key_length = is.ReadInt32();
    
            LOG(INFO) << "num key count " << num_key_length;
            LOG(INFO) << "max key length " << max_key_length;
   
            if (!IsNoDataSection())
            {
                data_offsets_.resize(max_key_length+1, 0);

                for (int32_t i = 0;i < num_key_length; i++)
                {
                    auto len = is.ReadInt32();
                    data_offsets_[len] = is.ReadInt64();
                }
            }
    
            pfd_offset = is.ReadInt32();
            trie_offset = is.ReadInt32();
            data_offset = is.ReadInt64();

            // Must Load pfd first
            if (!IsNoDataSection())
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
            LOG(FATAL) << "Checksum not matched: " << fname;
        }

        fd_ = ::open(fname.c_str(), O_RDONLY);
        CHECK(fd_) << "open " << fname << " failed";

        FileUtil::GetFileSize(fname, &length_);

        auto page_size = sysconf(_SC_PAGE_SIZE);
        auto offset = (trie_offset / page_size) * page_size;
        auto page_offset = trie_offset % page_size;
        ptr_ = reinterpret_cast<char*>(::mmap(NULL, length_, PROT_READ, MAP_SHARED, fd_, offset));

        index_ptr_ = ptr_ + page_offset;

        if (!IsNoDataSection())
        {
            data_ptr_ =  ptr_ + page_offset  + (data_offset - trie_offset);
        }
    
        trie_.map(index_ptr_, data_offset - trie_offset);
    }
    
    ~Impl()
    {
        ::munmap(ptr_, length_);
        ::close(fd_);
    }
  
    bool IsNoDataSection() const
    {
        if (writer_option_.build_type == 1)
            return true;

        if (writer_option_.compress_type == 2)
            return true;

        return false;
    }

    StringPiece GetValue(uint32_t id, size_t len) const
    {
        auto offset = pfd_.Extract(id);
        auto data_offset = data_offsets_[len];
        auto block_ptr = reinterpret_cast<const int8_t*>(data_ptr_ + data_offset + offset);

        size_t prefix_length;
        auto value_length = DecodeVarint(block_ptr, block_ptr + 10, &prefix_length);
        return StringPiece(reinterpret_cast<const char*>(block_ptr + prefix_length), value_length);
    }

    StringPiece GetInternal(const StringPiece& k) const
    {
        DCHECK(!IsNoDataSection()) << "Invalid Operation, No Value has been load!!!";

        StringPiece result("");
        marisa::Agent agent;
        agent.set_query(k.data(), k.length());
        if (!trie_.lookup(agent))
        {
            return result;
        }
  
        return GetValue(agent.key().id(), k.length());
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
                m.push_back(std::make_pair(std::string(keyset[i].ptr(), keyset[i].length()),
                                           GetValue(keyset[i].id(), keyset[i].length()).ToString()));
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
            }
        }
        catch (const marisa::Exception &ex)
        {
            LOG(ERROR) << ex.what() << ": PrefixGet() failed: "
                       << k.ToString();
        }

        return m;
    }

    StringPiece GetRawKey(const StringPiece& key) const
    {
        return GetInternal(key);
    }

    StringPiece GetPrefixKey(const StringPiece& key) const
    {
        StringPiece result("");

        marisa::Agent agent;
        agent.set_query(key.data(), key.length());

        while (trie_.predictive_search(agent))
        {
            if (agent.key()[key.length()] == '\t')
            {
                result.reset(agent.key().ptr()+key.length()+1, agent.key().length()-key.length()-1);
                break;
            }
        }
        return result;
    }

    std::string GetCompressedValueAsString(const StringPiece& key) const
    {
        auto v = GetInternal(key);

        std::string ucv;
        snappy::Uncompress(v.data(), v.length(), &ucv);
        return ucv;
    }

    bool ExistRawKey(const StringPiece& key) const
    {
        marisa::Agent agent;
        agent.set_query(key.data(), key.length());
        return !trie_.lookup(agent);
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
        if (writer_option_.compress_type == 2)
            return ExistPrefixKey(key);
        return ExistRawKey(key);
    }

    StringPiece Get(const StringPiece& key) const
    {
        if (writer_option_.build_type == 1)
        {
            return StringPiece("");
        }

        if (writer_option_.compress_type == 0)
        {
            return GetRawKey(key);
        }
        else if (writer_option_.compress_type == 2)
        {
            return GetPrefixKey(key);
        }

        return StringPiece("");
    }

    std::string GetAsString(const StringPiece& key) const
    {
        if (writer_option_.build_type == 1)
            return "";

        if (writer_option_.compress_type == 0)
            return GetRawKey(key).ToString();

        if (writer_option_.compress_type == 1)
            return GetCompressedValueAsString(key);
        return "";
    }

private:
    Reader::Option option_;
    Writer::Option writer_option_;

    int fd_;
    uint64_t length_;
    char* ptr_;

    std::vector<int64_t> data_offsets_;

    const char* index_ptr_;
    const char* data_ptr_;

    marisa::Trie trie_;
    PForDelta pfd_;
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
