#pragma once

#include <inttypes.h>

#include <string>

#include <boost/operators.hpp>

namespace scdb {

///
/// Time stamp in UTC, in microseconds resolution.
///
/// This class is immutable.
/// It's recommended to pass it by value, since it's passed in register on x64.
///
class Timestamp : boost::less_than_comparable<Timestamp>
{
public:
    static const int kMicroSecondsPerSecond = 1000 * 1000;

    ///
    /// Constucts an invalid Timestamp.
    ///
    Timestamp()
        : microseconds_since_epoch_(0) {}

    ///
    /// Constucts a Timestamp at specific time
    ///
    /// @param microseconds_since_epoch
    explicit Timestamp(int64_t microseconds_since_epoch)
        : microseconds_since_epoch_(microseconds_since_epoch) {}

    void swap(Timestamp& other)
    {
        std::swap(microseconds_since_epoch_, other.microseconds_since_epoch_);
    }

    std::string ToFormattedString() const;

    bool Valid() const
    {
        return microseconds_since_epoch_ > 0;
    }

    time_t SecondsSinceEpoch() const
    {
        return static_cast<time_t>(microseconds_since_epoch_/kMicroSecondsPerSecond);
    }

    int64_t MicroSecondsSinceEpoch() const
    {
        return microseconds_since_epoch_;
    }

    static Timestamp Now();
    static Timestamp Invalid()
    {
        return Timestamp();
    }

private:
    int64_t microseconds_since_epoch_;
};

inline bool operator<(Timestamp lhs, Timestamp rhs)
{
    return lhs.MicroSecondsSinceEpoch() < rhs.MicroSecondsSinceEpoch();
}

inline bool operator==(Timestamp lhs, Timestamp rhs)
{
    return lhs.MicroSecondsSinceEpoch() == rhs.MicroSecondsSinceEpoch();
}

/// Gets time difference of two timestamps, result in microseconds
inline int64_t TimeDifference(Timestamp high, Timestamp low)
{
    return (high.MicroSecondsSinceEpoch() - low.MicroSecondsSinceEpoch());
}

/// @return timestamp + microseconds
inline Timestamp AddTime(Timestamp timestamp, int64_t microseconds)
{
    return Timestamp(timestamp.MicroSecondsSinceEpoch() + microseconds);
}

} // namespace
