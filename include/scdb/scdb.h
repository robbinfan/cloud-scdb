#pragma once

#include "scdb/reader.h"
#include "scdb/writer.h"

namespace scdb {

Reader* CreateReader(const Reader::Option& option, const std::string& name);

Writer* CreateWriter(const Writer::Option& option, const std::string& name);

} // namespace
