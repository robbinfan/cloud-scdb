#pragma once

#include "scdb/reader.h"

#include <vector>

#include <boost/scoped_ptr.hpp>
#include <boost/noncopyable.hpp>

namespace scdb {

class HashReader : boost::noncopyable,
                   public Reader
{
public:
    HashReader(const Reader::Option& option, const std::string& fname); 
    virtual ~HashReader();

    virtual bool Exist(const StringPiece& s) const;

    virtual StringPiece Get(const StringPiece& s) const;
    virtual std::string GetAsString(const StringPiece& s) const;

private:
    class Impl; 
    boost::scoped_ptr<Impl> impl_;
};

} // namespace
