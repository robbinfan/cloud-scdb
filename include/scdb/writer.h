#pragma once

#include "scdb/string_piece.h"

namespace scdb {

class Writer
{
public:
    enum CompressType
    {
        kNone = 0,
        kSnappy = 1,
        kDFA = 2
    };

    enum BuildType
    {
        kMap = 0,
        kSet = 1,
    };

    struct Option
    {
        Option()
            : temp_folder("./tmp"),
              compress_type(kNone),
              build_type(kMap),
              with_checksum(false)
        {}

        bool IsNoDataSection() const
        {
            if (build_type == 1)
                return true;

            return false;
        }

        std::string temp_folder;
        CompressType compress_type;
        BuildType build_type;
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
