#pragma once

#include <stdint.h>
#include <endian.h>

namespace scdb {

#if defined(__clang__) || __GNUC_MINOR__ >= 6
#pragma GCC diagnostic push
#endif
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Wold-style-cast"
inline uint64_t HostToNetwork64(uint64_t host64)
{
    return htobe64(host64);
}

inline uint32_t HostToNetwork32(uint32_t host32)
{
    return htobe32(host32);
}

inline uint16_t HostToNetwork16(uint16_t host16)
{
    return htobe16(host16);
}

inline uint64_t NetworkToHost64(uint64_t net64)
{
    return be64toh(net64);
}

inline uint32_t NetworkToHost32(uint32_t net32)
{
    return be32toh(net32);
}

inline uint16_t NetworkToHost16(uint16_t net16)
{
    return be16toh(net16);
}
#if defined(__clang__) || __GNUC_MINOR__ >= 6
#pragma GCC diagnostic pop
#else
#pragma GCC diagnostic error "-Wconversion"
#pragma GCC diagnostic error "-Wold-style-cast"
#endif

} // namespace
