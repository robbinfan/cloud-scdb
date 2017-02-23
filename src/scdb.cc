#include "scdb/scdb.h"

#include <fstream>

#include "marisa-trie_reader.h"
#include "marisa-trie_writer.h"

#include <glog/logging.h>

namespace scdb {

Reader* CreateReader(const Reader::Option& option, const std::string& input)
{
    std::ifstream is(input, std::ios::binary);
    if (!is.is_open())
    {
        return NULL;
    }

    char buf[7];
    is.read(buf, sizeof buf);
    is.close();

    if (strncmp(buf, "SCDBV1.", 7))
    {
        return NULL;
    }

    Reader* reader =  NULL;
    try
    {
        reader = new MarisaTrieReader(option, input);
    }
    catch (const std::exception& e)
    {
        DLOG(ERROR) << "make writer failed: " << e.what();
    }

    return reader;
}

Writer* CreateWriter(const Writer::Option& option, const std::string& output)
{
    return new MarisaTrieWriter(option, output);
}

} // namespace
