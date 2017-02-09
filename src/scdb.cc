#include "scdb/scdb.h"

#include <fstream>

#include "hash_reader.h"
#include "hash_writer.h"

#include "marisa-trie_reader.h"
#include "marisa-trie_writer.h"

namespace scdb {

Reader* CreateReader(const Reader::Option& option, const std::string& input)
{
    std::ifstream is(input, std::ios::binary);

    char buf[7];
    is.read(buf, sizeof buf);
    is.close();

    if (strncmp(buf, "SCDBV1.", 7) == 0)
    {
        return new HashReader(option, input);
    }
    else if (strncmp(buf, "SCDBV2.", 7) == 0)
    {
        return new MarisaTrieReader(option, input);
    }

    return NULL;
}

Writer* CreateWriter(const Writer::Option& option, const std::string& output)
{
    if (option.load_factor > 0.0f)
    {
        return new HashWriter(option, output);
    }
    return new MarisaTrieWriter(option, output);
}

} // namespace
