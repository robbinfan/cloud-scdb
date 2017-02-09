#pragma once

#include "scdb/writer.h"

#include <boost/scoped_ptr.hpp>
#include <boost/noncopyable.hpp>

namespace scdb {

class HashWriter : boost::noncopyable,
                   public Writer
{
public:
    HashWriter(const Writer::Option& option, const std::string& fname);
    virtual ~HashWriter();

    virtual void Put(const StringPiece& k);
    virtual void Put(const StringPiece& k, const StringPiece& v);
    virtual void Close();

private:
    class Impl;
    boost::scoped_ptr<Impl> impl_;
};

} // namespace
