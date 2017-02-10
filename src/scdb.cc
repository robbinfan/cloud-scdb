#include "scdb/scdb.h"

#include <fstream>

#include "marisa-trie_reader.h"
#include "marisa-trie_writer.h"

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

    if (strncmp(buf, "SCDBV1.", 7) == 0)
    {
        return new MarisaTrieReader(option, input);
    }

    return NULL;
}

Writer* CreateWriter(const Writer::Option& option, const std::string& output)
{
    return new MarisaTrieWriter(option, output);
}

} // namespace
