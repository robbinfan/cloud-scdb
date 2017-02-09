#pragma once

#include <boost/scoped_ptr.hpp>
#include <boost/noncopyable.hpp>

#include "utils/endian.h"
#include "utils/file_util.h"

namespace scdb {

class FileInputStream : boost::noncopyable
{
public:
    FileInputStream(const std::string& fname)
        : file_(new FileUtil::SequentialFile(fname))
    {
    }

    ~FileInputStream()
    {
    }

    size_t Read(char* s, size_t len)
    {
        StringPiece fragment;
        auto status = file_->Read(len, &fragment, s);
        if (status)
            return 0;
        return fragment.length();
    }

    size_t Read(std::vector<char>& v)
    {
        return Read(&v[0], v.size());
    }

    bool ReadBool()
    {
        bool v;
        Read(reinterpret_cast<char*>(&v), sizeof v);
        return v;
    }

    bool ReadInt8()
    {
        int8_t v;
        Read(reinterpret_cast<char*>(&v), sizeof v);
        return v;
    }

    int32_t ReadInt32()
    {
        int32_t v;
        Read(reinterpret_cast<char*>(&v), sizeof v);
        return NetworkToHost32(v);
    }

    int64_t ReadInt64()
    {
        int64_t v;
        Read(reinterpret_cast<char*>(&v), sizeof v);
        return NetworkToHost64(v);
    }

    void Back(size_t n)
    {
        file_->Back(n);
    }

private:
    boost::scoped_ptr<FileUtil::SequentialFile> file_;
};

class FileOutputStream : boost::noncopyable
{
public:
    FileOutputStream(const std::string& fname)
        : file_(new FileUtil::WritableFile(fname))
    {
    }

    ~FileOutputStream()
    {
    }

    void Close()
    {   
        file_->Close();
    }

    void Append(const StringPiece& v)
    {
        file_->Append(v);
    }
  
    void Append(const int8_t* buf, size_t n)
    {
        file_->Append(reinterpret_cast<const char*>(buf), n);
    } 

    void AppendBool(bool v)
    {
        Append(reinterpret_cast<int8_t*>(&v), sizeof v);
    }

    void AppendInt8(int8_t v)
    {
        Append(&v, sizeof v);
    } 

    void AppendInt32(int32_t v)
    {
        int32_t be32 = HostToNetwork32(v);
        file_->Append(reinterpret_cast<const char*>(&be32), sizeof be32);
    }

    void AppendInt64(int64_t v)
    {
        int64_t be64 = HostToNetwork64(v);
        file_->Append(reinterpret_cast<const char*>(&be64), sizeof be64);
    }

    size_t size() const
    {
        return file_->WrittenBytes();
    }

private:
    boost::scoped_ptr<FileUtil::WritableFile> file_;
};

} // namespace
