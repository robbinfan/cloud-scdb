#pragma once

#include <vector>
#include <stdexcept>

#include "scdb/string_piece.h"

namespace scdb {

class Reader
{
public:
    struct Option
    {
        Option()
            : mmap_preload(false)
        {}

        bool mmap_preload;
    };

    virtual ~Reader() {}

    // whether [key] exist
    virtual bool Exist(const StringPiece& key) const = 0; 

    // Get the value of [key](iff build with value)
    virtual StringPiece Get(const StringPiece& key) const = 0; 

    // for uncompressed values of [key], more copy and uncompress time than Get
    virtual std::string GetAsString(const StringPiece& key) const = 0; 

    // Get values of prefix, at most [count] results
    virtual std::vector<std::pair<std::string, StringPiece>> PrefixGet(const StringPiece& prefix, size_t count) const
    {
        throw std::runtime_error("Not Implemented");
    }

    // Get uncomressed values of prefix, more copy and uncomressed time than PrefixGet
    virtual std::vector<std::pair<std::string, std::string>> PrefixGetAsString(const StringPiece& prefix, size_t count) const
    {
        throw std::runtime_error("Not Implemented");
    }
};

} // namespace
