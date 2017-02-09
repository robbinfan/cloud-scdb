#include "utils/pfordelta.h"

#include <math.h>

#include <fstream>

#include <glog/logging.h>

namespace scdb {

namespace {

const char* kTag = "PFDV1.";

uint32_t GetLgNum(uint64_t n)
{
    return 1 + log(n)/log(2);
}

uint32_t GetArraySize(uint64_t num, uint32_t bits)
{
    auto n = num*bits/64;
    if (n*64 < num*bits)
        n++;
    return n;
}

} // namespace

PForDelta::PForDelta(const std::vector<uint64_t>& v)
    : PForDelta()
{
    uint64_t max = 0;
    min_ = max = v[0];

    std::vector<uint64_t> count_lg(65);
    std::vector<uint64_t> min_lg(65, 0xffffffff);
    std::vector<uint64_t> max_lg(65, 0);
    for (auto& num : v)
    {
        if (num < min_)
            min_ = num;

        if (num > max)
            max = num;

        auto lg = 1 + static_cast<uint32_t>(log(num)/log(2));
        count_lg[lg]++;

        if (num < min_lg[lg])
            min_lg[lg] = num;
        if (num > max_lg[lg])
            max_lg[lg] = num;
    }
    min_bits_ = GetLgNum(min_);
    max_bits_ = GetLgNum(max);

    DLOG(INFO) << "Minimum/Maximum values in v[]: " << min_ << "/" << max << ", min bits = " << min_bits_ << ", max bits = "<< max_bits_;

    auto best_bits = v.size() * max_bits_;

    uint64_t count = 0;
    uint64_t total_bits = 0;
    uint64_t aux_min = 0;
    bool encoding = false;
    for (auto i = min_bits_; i < max_bits_; i++)
    {
        if (!count_lg[i])
            continue;

        count += count_lg[i];
        auto x = max_lg[i] - min_;
        auto lgx = GetLgNum(x);
        total_bits = count * lgx;

        if (count_lg[i+1])
            aux_min = min_lg[i+1];
        else
        {
            auto j = i + 2;
            for (;count_lg[j]== 0; j++);
            if (count_lg[j]) aux_min = min_lg[j];
        }

        auto y = max - aux_min;
        auto lgy = GetLgNum(y);
        total_bits += (v.size() - count)* lgy;

        if (encoding == false || total_bits < best_bits)
        {
            best_bits = total_bits;
            b_ = lgx;
            
            bas_p_ = min_;
            lim_p_ = aux_min;
            num_p_ = count;

            num_except_max_ = v.size() - count;
            bits_except_max_ = lgy;

            num_except_min_ = 0;
            bits_except_min_ = 0;

            encoding = true;
        }
    }

    DLOG_IF(INFO, encoding) << " *** [" << bas_p_ << ", " << lim_p_ << ") at the left: num of p = " << num_p_ << ", b = " << b_ << ", bits of except max = " << bits_except_max_ << ", best bits = " << best_bits;

    count = 0;
    uint64_t aux_max = 0;
    for (auto i = max_bits_; i > min_bits_; i--)
    {
        if (!count_lg[i])
            continue;

        count += count_lg[i];
        auto x = max - min_lg[i];
        auto lgx = GetLgNum(x);
        total_bits = count * lgx;

        if (max_lg[i-1])
            aux_max = max_lg[i-1];
        else if (i >= 2)
        {
            int j = i - 2;
            for (;j >= static_cast<int>(min_bits_)&& count_lg[j]==0;j--);
            if (j && count_lg[j])
                aux_max = max_lg[j];
        }

        auto y  = aux_max - min_ ;
        auto lgy = GetLgNum(y);
        total_bits += (v.size()- count) * lgy;

        if (encoding == false || total_bits < best_bits)
        {
            best_bits = total_bits;
            b_ = lgx;
            
            bas_p_ = min_lg[i];
            lim_p_ = max;
            num_p_ = count;

            num_except_max_ = 0;
            bits_except_max_ = 0;

            num_except_min_ = v.size() - count;
            bits_except_min_ = lgy;

            encoding = true;
        }
    }

    DLOG_IF(INFO, num_except_min_ > 0) << " *** [" << bas_p_ << ", " << lim_p_ << ") at the Right: num of p = " << num_p_ << ", b = " << b_ << ", bits except min = " << bits_except_min_ << ", best bits = " << best_bits;

    for (auto i = min_bits_+1; i < max_bits_; i++)
    {
        if (!count_lg[i])
            continue;

        for (auto j = i; j < max_bits_; j++)
        {
            if (!count_lg[j]) 
                continue;

            count = 0;
            for(auto k = i; k <= j; k++)
                count += count_lg[k];

            auto count2 = 0;
            for(auto k = min_bits_; k < i; k++)
                count2 += count_lg[k];

            if (count_lg[i-1])
                aux_max = max_lg[i-1];
            else if (i >= 2)
            {
                int m = i - 2;
                for (;m >= static_cast<int>(min_bits_) && count_lg[m] == 0; m--);
                if (m && count_lg[m])
                    aux_max = max_lg[m];
            }

            auto y = aux_max - min_;
            auto lgy = GetLgNum(y);
            total_bits = count2*lgy;

            auto x = max_lg[j] - min_lg[i];
            auto lgx = GetLgNum(x);
            total_bits += count*lgx;

            if (count_lg[j+1])
                aux_min = min_lg[j+1];
            else
            {
                auto m = j + 2;
                for (;m <= max_bits_ && count_lg[m]==0; m++);
                if (count_lg[m])
                    aux_min = min_lg[m];
            }

            auto except_max = max - aux_min;
            auto lgemax = GetLgNum(except_max);
            total_bits += (v.size() - count - count2) * lgemax + (v.size() - count) * 1.1;

            if (total_bits < best_bits)
            {
                best_bits = total_bits;
                b_ = lgx;

                bas_p_ = min_lg[i];
                lim_p_ = aux_min;
                num_p_ = count;

                num_except_min_ = count2;
                num_except_max_ = v.size() - count - count2;

                bits_except_min_ = lgy;
                bits_except_max_ = lgemax;

                is_except_ = true;
            }
        }
    }

    DLOG_IF(INFO, is_except_) << " *** [" << bas_p_ << ", " << lim_p_ << ") at the middle: num of p = " << num_p_ << ", num of except min = " << num_except_min_ << ", num of except max = " << num_except_max_;
    DLOG_IF(INFO, is_except_) << "     b = " << b_ << ", bits of except min = " << bits_except_min_ << ", bits of except max = " << bits_except_max_ << ", best bits = " << best_bits;

    //auto bytes_v = (v.size() * max_bits_)/8;
    auto bytes_v = v.size() * 8;
    size_t bytes_pfd = 0;
    if (num_except_min_ || num_except_max_)
    {
        auto bv = sdsl::bit_vector(v.size(), 0);
        if (is_except_)
            except_bv_ = sdsl::bit_vector(v.size() - num_p_, 0);

        auto n = num_p_*b_/64;
        if ((num_p_*b_) % 64)
            n++;
        p_ = new uint64_t[n];
        bytes_pfd = n*8;

        DLOG(INFO) << " ** size of p[ ] : " << bytes_pfd << " Bytes = " << bytes_pfd/static_cast<float>(bytes_v) << "|v|";

        n = GetArraySize(num_except_min_, bits_except_min_);
        if (n)
        {
            except_min_ = new uint64_t[n];
            bytes_pfd += n*8;
            DLOG(INFO) << " ** size of except min[ ] : " << n*8 << " Bytes = " << n*8/static_cast<float>(bytes_v) << "|v|";
        }

        n = GetArraySize(num_except_max_, bits_except_max_);
        if (n)
        {
            except_max_ = new uint64_t[n];
            bytes_pfd += n*8;
            DLOG(INFO) << " ** size of except max[ ] : " << n*8 << " Bytes = " << n*8/static_cast<float>(bytes_v) << "|v|";
        }

        uint64_t n_min, n_max, n_ex;
        n_min = n_max = n_ex = 0;
        for (uint64_t i = 0, j = 0;i < v.size(); i++)
        {
            auto& num = v[i];
            if (bas_p_ <= num && num < lim_p_)
            {
                bv[i] = 1;
                SetNum64(p_, j, b_, num-bas_p_);
                j += b_;
            }
            else
            {
                if (is_except_)
                {
                    if (num < bas_p_)
                    {
                        except_bv_[n_ex] = 1;
                        SetNum64(except_min_, n_min, bits_except_min_, num-min_);
                        n_min += bits_except_min_;
                    }
                    else
                    {
                        SetNum64(except_max_, n_max, bits_except_max_, num-lim_p_);
                        n_max += bits_except_max_;
                    }
                    n_ex++;
                }
                else
                {
                    if (num_except_min_)
                    {
                        SetNum64(except_min_, n_min, bits_except_min_, num-min_);
                        n_min += bits_except_min_;
                    }
                    else
                    {
                        SetNum64(except_max_, n_max, bits_except_max_, num-lim_p_);
                        n_max += bits_except_max_;
                    }
                }
            }
        }

        packed_rrr_ = sdsl::rrr_vector<127>(bv);
        bytes_pfd += size_in_bytes(packed_rrr_);
        DLOG(INFO) << " ** size of packed_rrr: " << size_in_bytes(packed_rrr_) << " = " << size_in_bytes(packed_rrr_)/static_cast<float>(bytes_v) << "|v|";
        packed_rank_ = sdsl::rrr_vector<127>::rank_1_type(&packed_rrr_); // 1/4 size of bit vector

        decltype(bv) empty;
        bv.swap(empty);

        if (is_except_)
        {
            bytes_pfd += size_in_bytes(except_bv_);
            DLOG(INFO) << " ** size of except bv: " << size_in_bytes(except_bv_) << " = " << size_in_bytes(except_bv_)/static_cast<float>(bytes_v) << "|v|";

            except_rank_ = sdsl::rank_support_v<>(&except_bv_);
            bytes_pfd += size_in_bytes(except_rank_);
            DLOG(INFO) << " ** size of except_rank_: " << size_in_bytes(except_rank_) << " = " << size_in_bytes(except_rank_)/static_cast<float>(bytes_v) << "|v|";
        }

        DLOG_IF(INFO, bytes_pfd > bytes_v) << "WARNING! PForDelta does not work well for the probability of distribution of the input array, But we have compressed it anyway ! ";

    }
    else
    {
        DLOG(INFO) << "WARNING! PForDelta does not work well for the probability of distribution of the input array, But we have compressed it anyway ! ";

        auto bv = sdsl::bit_vector(v.size(), 1);
        bas_p_ = min_;
        lim_p_ = max;
        b_ = GetLgNum(lim_p_ - bas_p_);

        auto n = GetArraySize(num_p_, b_);
        p_ = new uint64_t[n];
        bytes_pfd = n*sizeof(uint64_t);
        DLOG(INFO) << " ** size of P[ ] : " << n*sizeof(uint64_t) << " Bytes = " << n*sizeof(uint64_t)/static_cast<float>(bytes_v) << "|v|";
        
        n = 0;
        for (auto& num : v)
        {
            SetNum64(p_, n, b_, num-bas_p_);
            n += b_;
        }

        packed_rrr_ = sdsl::rrr_vector<127>(bv);
        bytes_pfd += size_in_bytes(packed_rrr_);
        DLOG(INFO) << " ** size of packed_rrr: " << size_in_bytes(packed_rrr_) << " = " << size_in_bytes(packed_rrr_)/static_cast<float>(bytes_v) << "|v|";
        packed_rank_ = sdsl::rrr_vector<127>::rank_1_type(&packed_rrr_);

        decltype(bv) empty;
        bv.swap(empty);
    }

    DLOG(INFO) << "Size (in bytes) of PForDelta = " << bytes_pfd << " vs " << bytes_v << " of the input |v|";
    DLOG(INFO) << "Compress Ratio: " << static_cast<float>(bytes_pfd)/static_cast<float>(bytes_v); 

#ifndef NDEBUG
    Test(v);
#endif
}

void PForDelta::Test(const std::vector<uint64_t>& v)
{
    DLOG(INFO) << "Testing Extract v[i] ...";
    for (uint64_t i = 0; i < v.size() ; i++)
    {
        auto num = Extract(i);
        if (v[i] != num)
            DLOG(FATAL) << " x = " << num << " != v[" << i << "] = " << v[i];
    }
    DLOG(INFO) << "Test OK !!";
}

void PForDelta::SetNum64(uint64_t* A, uint64_t start, uint32_t length, uint64_t x)
{
    if (!length) return ;
    auto i = start >> 6u;
    auto j = start - (i << 6u);

    if ((j + length > 64u))
    {
        auto mask = ~(~0ul >> j);
        A[i] = (A[i] & mask) | (x >> (j + length - 64u));
        mask = ~0ul >> (j + length - 64u);
        A[i+1] = (A[i+1] & mask) | (x << (128u - j - length));
    }
    else
    {
        auto mask = (~0ul >> j) ^ (~0ul << (64u - j - length)); // XOR: 1^1=0^0=0; 0^1=1^0=1
        A[i] = (A[i] & mask) | (x << (64u - j - length));
    }
}

uint64_t PForDelta::GetNum64(uint64_t* A, uint64_t start, uint32_t length) const
{
    if (!length) return 0;

    auto i = start >> 6u;
    auto j = start - (i << 6u);
    auto result = (A[i] << j) >> (64u - length);

    if ((j + length) > 64u)
        result = result | (A[i+1] >> (128u - j - length));

    return result;
}

uint64_t PForDelta::Extract(uint64_t idx) const
{
    auto r = packed_rank_(idx+1);
    if (packed_rrr_[idx])
    {
        return bas_p_+GetNum64(p_, (r-1)*b_, b_);
    }
    else
    {
        auto except_idx = idx + 1 - r;
        if (is_except_)
        {
            auto ii = except_rank_.rank(except_idx);

            if (except_bv_[except_idx-1])
            {
                auto n = GetNum64(except_min_, (ii-1)*bits_except_min_, bits_except_min_);
                return min_ + n;
            }
            else
                return lim_p_+GetNum64(except_max_, (except_idx-ii-1)*bits_except_max_, bits_except_max_);
        }
        else
        {
            if (num_except_min_)
                return min_+GetNum64(except_min_, (except_idx-1)*bits_except_min_, bits_except_min_);
            else
                return lim_p_+GetNum64(except_max_, (except_idx-1)*bits_except_max_, bits_except_max_);
        }
    }

    return 0;
}

void PForDelta::Save(const std::string& fname)
{
    std::ofstream os(fname, std::ios::binary);

    os.write(kTag, strlen(kTag));

    os.write((const char*)&num_p_, sizeof(uint64_t));
    os.write((const char*)&num_except_min_, sizeof(uint64_t));
    os.write((const char*)&num_except_max_, sizeof(uint64_t));

    os.write((const char*)&min_, sizeof(uint64_t));
    os.write((const char*)&bas_p_, sizeof(uint64_t));
    os.write((const char*)&lim_p_, sizeof(uint64_t));

    os.write((const char*)&min_bits_, sizeof(uint32_t));
    os.write((const char*)&max_bits_, sizeof(uint32_t));
    os.write((const char*)&bits_except_min_, sizeof(uint32_t));
    os.write((const char*)&b_, sizeof(uint32_t));
    os.write((const char*)&bits_except_max_, sizeof(uint32_t));

    os.write((const char*)&is_except_, sizeof(bool));

    auto n = GetArraySize(num_p_, b_);
    if (n)
    {
        os.write((const char*)p_, n*sizeof(uint64_t));
        DLOG(INFO) << " .- P[] " << n*sizeof(uint64_t) << " Bytes";
    }

    n = GetArraySize(num_except_min_, bits_except_min_);
    if (n)
    {
        os.write((const char*)except_min_, n*sizeof(uint64_t));
        DLOG(INFO) << " .- except min [] " << n*sizeof(uint64_t) << " Bytes";
    }

    n = GetArraySize(num_except_max_, bits_except_max_);
    if (num_except_max_)
    {
        os.write((const char*)except_max_, n*sizeof(uint64_t));
        DLOG(INFO) << " .- except max[] " << n*sizeof(uint64_t) << " Bytes";
    }

    packed_rrr_.serialize(os);
    DLOG(INFO) << " ** size of packed_rrr " << size_in_bytes(packed_rrr_) << " Bytes";

    packed_rank_.serialize(os);

    if(is_except_)
    {
        except_bv_.serialize(os);
        except_rank_.serialize(os);
    }

    os.close();

    DLOG(INFO) << "   PForDelta Saved !\n"
               << "______________________________________________________________";
}

void PForDelta::Load(const std::string& fname, size_t offset)
{
    std::ifstream is(fname, std::ios::binary);
    is.seekg(offset, std::ios::beg);
    
    char buf[6];
    is.read(buf, sizeof buf);
    if (strncmp(buf, kTag, sizeof buf))
        DLOG(FATAL) << "Invalid PFD Data";

    is.read((char*)&num_p_, sizeof(uint64_t));
    is.read((char*)&num_except_min_, sizeof(uint64_t));
    is.read((char*)&num_except_max_, sizeof(uint64_t));

    is.read((char*)&min_, sizeof(uint64_t));
    is.read((char*)&bas_p_, sizeof(uint64_t));
    is.read((char*)&lim_p_, sizeof(uint64_t));

    is.read((char*)&min_bits_, sizeof(uint32_t));
    is.read((char*)&max_bits_, sizeof(uint32_t));
    is.read((char*)&bits_except_min_, sizeof(uint32_t));
    is.read((char*)&b_, sizeof(uint32_t));
    is.read((char*)&bits_except_max_, sizeof(uint32_t));

    is.read((char*)&is_except_, sizeof(bool));

    auto n = GetArraySize(num_p_, b_);
    if (n)
    {
        p_ = new uint64_t[n];
        is.read((char*)p_, n*sizeof(uint64_t));
        DLOG(INFO) << " ** size of P[] " << n*sizeof(uint64_t) << " Bytes";
    }

    n = GetArraySize(num_except_min_, bits_except_min_);
    if (n)
    {
        except_min_ = new uint64_t[n];
        is.read((char*)except_min_, n*sizeof(uint64_t));
        DLOG(INFO) << " ** size of except min[] " << n*sizeof(uint64_t) << " Bytes";
    }

    n = GetArraySize(num_except_max_, bits_except_max_);
    if (n)
    {
        except_max_ = new uint64_t[n];
        is.read((char*)except_max_, n*sizeof(uint64_t));
        DLOG(INFO) << " ** size of except max[] " << n*sizeof(uint64_t) << " Bytes";
    }

    packed_rrr_.load(is);
    DLOG(INFO) << " ** size of packed rrr " << size_in_bytes(packed_rrr_) << " Bytes";

    packed_rank_.load(is);
    sdsl::util::init_support(packed_rank_, &packed_rrr_);

    if(is_except_)
    {
        except_bv_.load(is);
        except_rank_.load(is);
        sdsl::util::init_support(except_rank_, &except_bv_);
    }

    DLOG(INFO) << "   PForDelta Loaded\n"
               << "______________________________________________________________";

}

PForDelta::~PForDelta() 
{
    delete [] p_;

    delete [] except_min_;
    delete [] except_max_;

    decltype(packed_rrr_) empty;
    packed_rrr_.swap(empty);

    if (is_except_)
    {
        decltype(except_bv_) empty_ex;
        except_bv_.swap(empty_ex);

        decltype(except_rank_) empty_exr;
        except_rank_.swap(empty_exr);
    }
}

} // namespace
