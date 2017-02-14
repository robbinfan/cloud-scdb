#pragma once

#include <vector>

#include <sdsl/bit_vectors.hpp>

namespace scdb {

class PForDelta 
{
public:
    PForDelta()
        : p_(NULL),
          num_p_(0),
          bas_p_(0),
          lim_p_(0),
          is_except_(false),
          except_min_(NULL),
          except_max_(NULL),
          bits_except_min_(0),
          bits_except_max_(0),
          num_except_min_(0),
          num_except_max_(0),
          b_(0),
          min_bits_(0),
          max_bits_(0),
          min_(0),
          extract_except_func_(NULL)
    {}

	PForDelta(const std::vector<uint64_t>& v);

    virtual ~PForDelta();

    void Save(const std::string& fname);
	void Load(const std::string& fname, size_t offset = 0);

	uint64_t Extract(uint64_t i) const;
	void Test(const std::vector<uint64_t>& v);

private:

	// set the number x as a bitstring sequence in *A. In the range of bits [ini, .. ini+len-1] of *A. Here x has len bits
	void SetNum64(uint64_t *A, uint64_t ini, uint32_t len, uint64_t x);

	// return (in a unsigned long integer) the number in A from bits of position 'ini' to 'ini+len-1'
	uint64_t GetNum64(uint64_t *A, uint64_t ini, uint32_t len) const;

    uint64_t ExtractExcept(uint64_t) const;
    uint64_t ExtractExceptMin(uint64_t) const;
    uint64_t ExtractExceptMax(uint64_t) const;

private:
    sdsl::rrr_vector<127> bv_rrr_;
    sdsl::rrr_vector<127>::rank_1_type bv_rank_;

    sdsl::bit_vector except_bv_;
    sdsl::rank_support_v<> except_rank_;

    // [bas_p_, lim_p_) of p
	uint64_t* p_;
	uint64_t num_p_;
	uint64_t bas_p_; 
	uint64_t lim_p_;

	bool is_except_;
	uint64_t *except_min_;
	uint64_t *except_max_;
	uint32_t bits_except_min_;
	uint32_t bits_except_max_;	
	uint64_t num_except_min_;	
	uint64_t num_except_max_;

	uint32_t b_;
	uint32_t min_bits_;
	uint32_t max_bits_;
    uint64_t min_;

    typedef uint64_t(PForDelta::*ExtractExceptFunc)(uint64_t) const;
    ExtractExceptFunc extract_except_func_;
};

} // namespace
