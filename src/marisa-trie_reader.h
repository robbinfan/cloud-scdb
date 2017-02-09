#pragma once

#include "scdb/reader.h"

#include <boost/scoped_ptr.hpp>
#include <boost/noncopyable.hpp>

namespace scdb {

class MarisaTrieReader : boost::noncopyable,
                         public Reader
{
public:
    MarisaTrieReader(const Reader::Option& option, const std::string& fname); 
    virtual ~MarisaTrieReader();

    virtual bool Exist(const StringPiece& k) const;

    virtual StringPiece Get(const StringPiece& k) const;
    virtual std::string GetAsString(const StringPiece& k) const;

    virtual std::vector<std::pair<std::string, StringPiece>> PrefixGet(const StringPiece& prefix, size_t count) const;
    virtual std::vector<std::pair<std::string, std::string>> PrefixGetAsString(const StringPiece& prefix, size_t count) const;

private:
    class Impl;
    boost::scoped_ptr<Impl> impl_;
};

} // namespace
