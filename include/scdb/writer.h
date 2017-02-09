#pragma once

#include "scdb/string_piece.h"

namespace scdb {

class Writer
{
public:
    struct Option
    {
        Option()
            : temp_folder("./tmp"),
              load_factor(0.0f),
              compress_type(0),
              build_type(0),
              with_checksum(false)
        {}

        std::string temp_folder;
        double load_factor; // for hash reader only
        int8_t compress_type; // 0: no compress, 1: snappy, 2: trie
        int8_t build_type; // 0: k and v (as map), 1: only k(as set)
        bool with_checksum; // a checksum attached at endof file, will check when reader load
    };

    virtual ~Writer() {}

    // Build only k
    virtual void Put(const StringPiece& k) = 0;

    // Build k-v 
    virtual void Put(const StringPiece& k, const StringPiece& v) = 0;

    // Generate final output
    virtual void Close() = 0;
};

} // namespace
