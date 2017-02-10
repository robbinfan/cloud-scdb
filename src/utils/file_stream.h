#pragma once

#include <boost/scoped_ptr.hpp>
#include <boost/noncopyable.hpp>

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

    template<typename T>
    T Read()
    {
        T v;
        Read(reinterpret_cast<char*>(&v), sizeof v);
        return v;
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

    void Append(const int8_t* buf, size_t n)
    {
        file_->Append(reinterpret_cast<const char*>(buf), n);
    } 

    template<typename T>
    void Append(T v)
    {
        file_->Append(reinterpret_cast<const char*>(&v), sizeof v);
    }

    void Append(const std::string& str)
    {
        file_->Append(str.data(), str.length());
    }

    void Append(const char* s)
    {
        file_->Append(StringPiece(s));
    }

    void Append(const StringPiece& v)
    {
        file_->Append(v);
    }

    size_t size() const
    {
        return file_->WrittenBytes();
    }

private:
    boost::scoped_ptr<FileUtil::WritableFile> file_;
};

} // namespace
