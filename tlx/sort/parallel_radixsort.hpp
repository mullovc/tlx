/*******************************************************************************
 * src/parallel/bingmann-parallel_radix_sort.hpp
 *
 * Parallel radix sort with work-balancing.
 *
 * The set of strings is sorted using a 8- or 16-bit radix sort
 * algorithm. Recursive sorts are processed in parallel using a lock-free job
 * queue and OpenMP threads. Two radix sort implementations are used:
 * sequential in-place and parallelized out-of-place.
 *
 * The sequential radix sort is implemented using an explicit recursion stack,
 * which enables threads to "free up" work from the top of the stack when other
 * threads become idle. This variant uses in-place permuting and a character
 * cache (oracle).
 *
 * To parallelize sorting of large buckets an out-of-place variant is
 * implemented using a sequence of three Jobs: count, distribute and
 * copyback. All threads work on a job-specific context called BigRadixStepCE,
 * which encapsules all variables of an 8-bit or 16-bit radix sort (templatized
 * with key_type).
 *
 *******************************************************************************
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/

#ifndef PSS_SRC_PARALLEL_BINGMANN_PARALLEL_RADIX_SORT_HEADER
#define PSS_SRC_PARALLEL_BINGMANN_PARALLEL_RADIX_SORT_HEADER

#include <cstdlib>
#include <cstring>

#include <iostream>
#include <vector>


#include <tlx/sort/shadow_set.hpp>

// #include <tlx/sort/strings/string_set.hpp>
// #include <tlx/sort/strings/string_ptr.hpp>
// #include <tlx/sort/strings/insertion_sort.hpp>

#include <tlx/logger.hpp>
#include <tlx/thread_pool.hpp>
#include <tlx/multi_timer.hpp>
#include <tlx/die.hpp>

namespace tlx {
namespace parallel_radixsort_detail {

template <typename ValueType, typename KeyType>
inline
KeyType get_key(const ValueType v, size_t depth)
{
    // FIXME find endianess independent solution
    // return *((reinterpret_cast<const KeyType *>(&v)) + depth);
    return *((reinterpret_cast<const KeyType *>(&v)) + sizeof(ValueType) - depth - 1);
}



/// Return traits of key_type
template <typename CharT>
class key_traits
{ };

template <>
class key_traits<uint8_t>
{
public:
    static const size_t radix = 256;
    static const size_t add_depth = 1;
};

template <>
class key_traits<uint16_t>
{
public:
    static const size_t radix = 65536;
    static const size_t add_depth = 2;
};

/******************************************************************************/
//! Parallel Radix Sort Parameter Struct

template <typename value_type_, typename key_type_>
class PRSParametersDefault
{
public:
    // typedef Comparator_ Comparator;
    // typedef SubSorter_  SubSorter;

    //! key type for radix sort: 8-bit or 16-bit
    typedef value_type_ value_type;

    //! data type for radix sort: int32_t, int64_t, float,...
    typedef key_type_  key_type;

    static const bool debug_steps = false;
    static const bool debug_jobs = false;

    static const bool debug_bucket_size = false;
    static const bool debug_recursion = false;

    static const bool debug_result = false;

    //! enable work freeing
    static const bool enable_work_sharing = true;

    //! whether the base sequential_threshold() on the remaining unsorted data
    //! set or on the whole data set.
    static const bool enable_rest_size = false;

    //! threshold to run sequential small sorts
    // static const size_t smallsort_threshold = 1024 * 1024;
    //! threshold to switch to insertion sort
    static const size_t subsort_threshold = 32;

    //! sub-sorter
    // SubSorter sub_sort = std::sort;
    // constexpr static auto sub_sort = std::sort<value_type>;

    //! comparator for the sub-sorter
    // Comparator cmp = std::less<value_type>();
    // constexpr static auto cmp = std::less<value_type>();
};


/******************************************************************************/
//! Parallel Radix Sort Context

template <typename Parameters>
class PRSContext : public Parameters
{
public:
    // using Comparator = typename Parameters::Comparator;
    // using SubSorter  = typename Parameters::SubSorter;

    //! total size of input
    size_t totalsize;

    //! number of remaining elements to sort
    std::atomic<size_t> rest_size;

    //! counters
    std::atomic<size_t> para_ss_steps, sequ_ss_steps, base_sort_steps;

    //! timers for individual sorting steps
    MultiTimer mtimer;

    //! number of threads overall
    size_t num_threads;

    //! thread pool
    ThreadPool threads_;

    //! context constructor
    PRSContext(size_t _thread_num)
        : para_ss_steps(0), sequ_ss_steps(0), base_sort_steps(0),
          num_threads(_thread_num),
          threads_(_thread_num)
    { }

    //! enqueue a new job in the thread pool
    template <typename DataPtr>
    void enqueue(const DataPtr& dptr, size_t depth);

    //! return sequential sorting threshold
    size_t sequential_threshold() {
        // size_t threshold = this->smallsort_threshold;
        size_t threshold = this->subsort_threshold;
        if (this->enable_rest_size) {
            return std::max(threshold, rest_size / num_threads);
        }
        else {
            return std::max(threshold, totalsize / num_threads);
        }
    }

    //! decrement number of unordered elements
    void donesize(size_t n) {
        // FIXME use this somewhere
        if (this->enable_rest_size)
            rest_size -= n;
    }
};

// ****************************************************************************
// *** SmallsortJob8 - sort 8-bit radix in-place with explicit stack-based recursion

template <typename Context, typename DataPtr>
void EnqueueSmallsortJob8(Context& ctx, const DataPtr& dptr, size_t depth);

template <typename Context, typename BktSizeType, typename DataPtr>
struct SmallsortJob8 final
{
    DataPtr   dptr;
    size_t    depth;

    typedef BktSizeType bktsize_type;

    typedef typename Context::key_type key_type;
    typedef typename Context::value_type value_type;

    typedef typename DataPtr::DataSet DataSet;
    typedef typename DataSet::Iterator Iterator;

    SmallsortJob8(Context& ctx, const DataPtr& _dptr, size_t _depth)
        : dptr(_dptr), depth(_depth)
    {
        ctx.threads_.enqueue([this, &ctx]() { this->run(ctx); });
    }

    struct RadixStep8_CI
    {
        DataPtr      dptr;
        size_t       idx;
        bktsize_type bkt[256 + 1];

        RadixStep8_CI(const DataPtr& _dptr, size_t depth, key_type* charcache)
            : dptr(_dptr)
        {
            // using T = typename std::iterator_traits<Iterator>::value_type;
            DataSet ds = dptr.active();
            size_t n = ds.size();

            // count character occurances
            bktsize_type bktsize[256];
#if 1
            // DONE: first variant to first fill charcache and then count. This is
            // 2x as fast as the second variant.
            key_type* cc = charcache;
            for (Iterator i = ds.begin(); i != ds.end(); ++i, ++cc)
                *cc = get_key<value_type, key_type>(*i, depth);
                // *cc = ds.get_uint8(*i, depth);

            memset(bktsize, 0, sizeof(bktsize));
            for (cc = charcache; cc != charcache + n; ++cc)
                ++bktsize[static_cast<size_t>(*cc)];
#else
            memset(bktsize, 0, sizeof(bktsize));
            Iterator it = ds.begin();
            for (size_t i = 0; i < n; ++i, ++it) {
                // ++bktsize[(charcache[i] = ds.get_uint8(*it, depth)];
                ++bktsize[(charcache[i] = get_key<value_type, key_type>(*it, depth)];
                           }
#endif
            // inclusive prefix sum
            bkt[0] = bktsize[0];
            bktsize_type last_bkt_size = bktsize[0];
            for (size_t i = 1; i < 256; ++i) {
                bkt[i] = bkt[i - 1] + bktsize[i];
                if (bktsize[i]) last_bkt_size = bktsize[i];
            }

            // premute in-place
            for (size_t i = 0, j; i < n - last_bkt_size; )
            {
                value_type perm = std::move(*(ds.begin() + i));
                key_type permch = charcache[i];
                while ((j = --bkt[static_cast<size_t>(permch)]) > i)
                {
                    std::swap(perm, *(ds.begin() + j));
                    std::swap(permch, charcache[j]);
                }
                *(ds.begin() + i) = std::move(perm);
                i += bktsize[static_cast<size_t>(permch)];
            }

            // fix prefix sum
            bkt[0] = 0;
            for (size_t i = 1; i <= 256; ++i) {
                bkt[i] = bkt[i - 1] + bktsize[i - 1];
            }
            assert(bkt[256] == n);

            idx = 0; // will increment to 1 on first process, bkt 0 is not sorted further
        }
    };

    virtual void run(Context& ctx)
    {
        size_t n = dptr.size();

        LOGC(ctx.debug_jobs)
            << "Process SmallsortJob8 " << this << " of size " << n;

        dptr = dptr.copy_back();

        if (n < ctx.subsort_threshold) {
            // FIXME start from depth `depth`
            // insertion_sort(dptr.copy_back(), depth, 0);
            DataSet ds = dptr.copy_back().active();
            std::sort<Iterator>(ds.begin(), ds.end());
            return;
        }

        key_type* charcache = new key_type[n];

        // std::deque is much slower than std::vector, so we use an artificial
        // pop_front variable.
        size_t pop_front = 0;
        std::vector<RadixStep8_CI> radixstack;
        radixstack.emplace_back(dptr, depth, charcache);

        while (radixstack.size() > pop_front)
        {
            while (radixstack.back().idx < 255)
            {
                RadixStep8_CI& rs = radixstack.back();
                size_t b = ++rs.idx; // process the bucket rs.idx

                size_t bktsize = rs.bkt[b + 1] - rs.bkt[b];

                if (bktsize == 0)
                    continue;
                else if (bktsize < ctx.subsort_threshold)
                {
                    // FIXME start from depth `depth`
                    DataSet ds = rs.dptr.sub(rs.bkt[b], bktsize).copy_back().active();
                    std::sort<Iterator>(ds.begin(), ds.end());
                    // insertion_sort(
                    //     rs.dptr.sub(rs.bkt[b], bktsize).copy_back(),
                    //     depth + radixstack.size(), 0);
                }
                else
                {
                    radixstack.emplace_back(rs.dptr.sub(rs.bkt[b], bktsize),
                                            depth + radixstack.size(),
                                            charcache);
                }

                if (ctx.enable_work_sharing && ctx.threads_.has_idle())
                {
                    // convert top level of stack into independent jobs
                    LOGC(ctx.debug_jobs)
                        << "Freeing top level of SmallsortJob8's radixsort stack";

                    RadixStep8_CI& rt = radixstack[pop_front];

                    while (rt.idx < 255)
                    {
                        b = ++rt.idx; // enqueue the bucket rt.idx

                        size_t bktsize_ = rt.bkt[b + 1] - rt.bkt[b];

                        if (bktsize_ == 0) continue;
                        EnqueueSmallsortJob8(
                            ctx, rt.dptr.sub(rt.bkt[b], bktsize_),
                            depth + pop_front);
                    }

                    // shorten the current stack
                    ++pop_front;
                }
            }
            radixstack.pop_back();
        }

        delete[] charcache;
        delete this;
    }
};

template <typename Context, typename DataPtr>
void EnqueueSmallsortJob8(Context& ctx, const DataPtr& dptr, size_t depth)
{
    if (dptr.size() < (1LLU << 32))
        new SmallsortJob8<Context, uint32_t, DataPtr>(ctx, dptr, depth);
    else
        new SmallsortJob8<Context, uint64_t, DataPtr>(ctx, dptr, depth);
}

// ****************************************************************************
// *** BigRadixStepCE out-of-place 8- or 16-bit parallel radix sort with Jobs

template <typename Context, typename DataPtr>
struct BigRadixStepCE
{
    typedef typename Context::key_type key_type;
    typedef typename Context::value_type value_type;

    typedef typename DataPtr::DataSet DataSet;
    typedef typename DataSet::Iterator Iterator;

    static const size_t numbkts = key_traits<key_type>::radix;

    DataPtr             dptr;
    size_t              depth;

    size_t              parts;
    size_t              psize;
    std::atomic<size_t> pwork;
    size_t              * bkt;

    key_type            * charcache;

    BigRadixStepCE(Context& ctx, const DataPtr& dptr, size_t _depth);

    void                count(size_t p, Context& ctx);
    void                count_finished(Context& ctx);

    void                distribute(size_t p, Context& ctx);
    void                distribute_finished(Context& ctx);
};

template <typename Context, typename DataPtr>
BigRadixStepCE<Context, DataPtr>::BigRadixStepCE(
    Context& ctx, const DataPtr& _dptr, size_t _depth)
    : dptr(_dptr), depth(_depth)
{
    size_t n = dptr.size();

    parts = (n + ctx.sequential_threshold() - 1) / ctx.sequential_threshold();
    if (parts == 0) parts = 1;

    psize = (n + parts - 1) / parts;

    LOGC(ctx.debug_jobs)
        << "Area split into " << parts << " parts of size " << psize;

    bkt = new size_t[numbkts * parts + 1];
    charcache = new key_type[n];

    // create worker jobs
    pwork = parts;
    for (size_t p = 0; p < parts; ++p)
        ctx.threads_.enqueue([this, p, &ctx]() { count(p, ctx); });
}

template <typename Context, typename DataPtr>
void BigRadixStepCE<Context, DataPtr>::count(size_t p, Context& ctx)
{
    LOGC(ctx.debug_jobs)
        << "Process CountJob " << p << " @ " << this;

    const DataSet& ss = dptr.active();
    Iterator strB = ss.begin() + p * psize;
    Iterator strE = ss.begin() + std::min((p + 1) * psize, dptr.size());
    if (strE < strB) strE = strB;

    key_type* mycache = charcache + p * psize;
    key_type* mycacheE = mycache + (strE - strB);

    // DONE: check if processor-local stack + copy is faster. On 48-core AMD
    // Opteron it is not faster to copy first. On 32-core Intel Xeon the second
    // loop is slightly faster.
#if 0
    for (Iterator str = strB; str != strE; ++str, ++mycache)
        *mycache = get_key<value_type, key_type>(*str, depth);

    size_t* mybkt = bkt + p * numbkts;
    memset(mybkt, 0, numbkts * sizeof(size_t));
    for (mycache = charcache + p * psize; mycache != mycacheE; ++mycache)
        ++mybkt[*mycache];
#else
    for (Iterator str = strB; str != strE; ++str, ++mycache)
        *mycache = get_key<value_type, key_type>(*str, depth);

    size_t mybkt[numbkts] = { 0 };
    for (mycache = charcache + p * psize; mycache != mycacheE; ++mycache)
        ++mybkt[*mycache];
    memcpy(bkt + p * numbkts, mybkt, sizeof(mybkt));
#endif

    if (--pwork == 0)
        count_finished(ctx);
}

template <typename Context, typename DataPtr>
void BigRadixStepCE<Context, DataPtr>::count_finished(Context& ctx)
{
    LOGC(ctx.debug_jobs)
        << "Finishing CountJob " << this << " with prefixsum";

    // inclusive prefix sum over bkt
    size_t sum = 0;
    for (size_t i = 0; i < numbkts; ++i)
    {
        for (size_t p = 0; p < parts; ++p)
        {
            bkt[p * numbkts + i] = (sum += bkt[p * numbkts + i]);
        }
    }
    assert(sum == dptr.size());

    // create new jobs
    pwork = parts;
    for (size_t p = 0; p < parts; ++p)
        ctx.threads_.enqueue([this, p, &ctx]() { distribute(p, ctx); });
}

template <typename Context, typename DataPtr>
void BigRadixStepCE<Context, DataPtr>::distribute(size_t p, Context& ctx)
{
    LOGC(ctx.debug_jobs)
        << "Process DistributeJob " << p << " @ " << this;

    const DataSet& ss = dptr.active();
    Iterator strB = ss.begin() + p * psize;
    Iterator strE = ss.begin() + std::min((p + 1) * psize, dptr.size());
    if (strE < strB) strE = strB;

    Iterator sorted = dptr.shadow().begin(); // get alternative shadow pointer array
    key_type* mycache = charcache + p * psize;

    // DONE: check if processor-local stack + copy is faster. On 48-core AMD
    // Opteron it is not faster to copy first. On 32-core Intel Xeon the second
    // loop is slightly faster.
#if 0
    size_t* mybkt = bkt + p * numbkts;

    for (Iterator str = strB; str != strE; ++str, ++mycache)
        sorted[--mybkt[*mycache]] = *str;
#else
    size_t mybkt[numbkts];
    memcpy(mybkt, bkt + p * numbkts, sizeof(mybkt));

    for (Iterator str = strB; str != strE; ++str, ++mycache)
        sorted[--mybkt[*mycache]] = std::move(*str);

    if (p == 0) // these are needed for recursion into bkts
        memcpy(bkt, mybkt, sizeof(mybkt));
#endif

    if (--pwork == 0)
        distribute_finished(ctx);
}

template <typename Context, typename DataPtr>
void BigRadixStepCE<Context, DataPtr>::distribute_finished(Context& ctx)
{
    LOGC(ctx.debug_jobs)
        << "Finishing DistributeJob " << this << " with enqueuing subjobs";

    delete[] charcache;

    if (sizeof(key_type) == 1)
    {
        // first p's bkt pointers are boundaries between bkts, just add sentinel:
        assert(bkt[0] == 0);
        bkt[numbkts] = dptr.size();

        // maybe copy back finished pointers from shadow
        dptr.flip(0, bkt[1] - bkt[0]).copy_back();

        for (size_t i = 1; i < numbkts; ++i)
        {
            if (bkt[i] == bkt[i + 1])
                continue;
            else if (bkt[i] + 1 == bkt[i + 1]) // just one element, copyback
                dptr.flip(bkt[i], 1).copy_back();
            else
                ctx.enqueue(dptr.flip(bkt[i], bkt[i + 1] - bkt[i]), depth + key_traits<key_type>::add_depth);
        }
    }
    else if (sizeof(key_type) == 2)
    {
        // first p's bkt pointers are boundaries between bkts, just add sentinel:
        assert(bkt[0] == 0);
        bkt[numbkts] = dptr.size();

        // maybe copy back finished pointers from shadow
        dptr.flip(0, bkt[0x0101] - bkt[0]).copy_back();

        for (size_t i = 0x0101; i < numbkts; ++i)
        {
            // skip over finished buckets 0x??00
            if ((i & 0x00FF) == 0) {
                dptr.flip(bkt[i], bkt[i + 1] - bkt[i]).copy_back();
                continue;
            }

            if (bkt[i] == bkt[i + 1])
                continue;
            else if (bkt[i] + 1 == bkt[i + 1]) // just one element, copyback
                dptr.flip(bkt[i], 1).copy_back();
            else
                ctx.enqueue(dptr.flip(bkt[i], bkt[i + 1] - bkt[i]), depth + key_traits<key_type>::add_depth);
        }
    }
    else
        die("impossible");

    delete[] bkt;
    delete this;
}

template <typename Parameters>
template <typename DataPtr>
void PRSContext<Parameters>::enqueue(const DataPtr& dptr, size_t depth)
{
    using Context = PRSContext<Parameters>;

    if (dptr.size() > this->sequential_threshold())
        new BigRadixStepCE<Context, DataPtr>(*this, dptr, depth);
    else
        EnqueueSmallsortJob8(*this, dptr, depth);
}

/******************************************************************************/
// Frontends


template <typename PRSParameters, typename Iterator>
static inline
void radix_sort_CI_params(Iterator begin, Iterator end, size_t MaxDepth)
{
    using Context = PRSContext<PRSParameters>;
    using T = typename std::iterator_traits<Iterator>::value_type;

    (void)MaxDepth;

    Context ctx(std::thread::hardware_concurrency());
    ctx.totalsize = end - begin;

    // allocate shadow pointer array
    T *shadow = new T[ctx.totalsize];
    Iterator shadow_begin = Iterator(shadow);

    // ShadowDataPtr ss(begin, end, shadow, shadow + ctx.totalsize);
    ctx.enqueue(ShadowDataPtr<DummyDataSet<Iterator>>(begin, end, shadow_begin, shadow_begin + ctx.totalsize), 0);

    // ctx.enqueue(StringShadowPtr<StringSet>(ss, StringSet(shadow)), depth);
    ctx.threads_.loop_until_empty();

    delete[] shadow;
}

/*!
 * Radix sort the iterator range [begin,end). Sort unconditionally up to depth
 * MaxDepth, then call the sub_sort method for further sorting. Small buckets
 * are sorted using std::sort() with given comparator. Characters are extracted
 * from items in the range using the at_radix(depth) method. All character
 * values must be less than K (the counting array size).
 */
template <typename Iterator>
static inline
void radix_sort_CI(Iterator begin, Iterator end, size_t MaxDepth)
{
    using Type = typename std::iterator_traits<Iterator>::value_type;

	radix_sort_CI_params<PRSParametersDefault<Type, uint8_t>, Iterator>(
            begin, end, MaxDepth);
}

// template <typename PS5Parameters, typename StringSet>
// void parallel_radix_sort_8bit_generic(const StringSet& ss, size_t depth = 0)
// {
//     using Context = PRSContext<PS5Parameters>;
//     Context ctx(std::thread::hardware_concurrency());
//     ctx.totalsize = ss.size();
//     ctx.num_threads = std::thread::hardware_concurrency();

//     // allocate shadow pointer array
//     typename StringSet::Container shadow = ss.allocate(ss.size());

//     ctx.enqueue(StringShadowPtr<StringSet>(ss, StringSet(shadow)), depth);
//     ctx.threads_.loop_until_empty();

//     StringSet::deallocate(shadow);
// }

} // namespace tlx
} // namespace parallel_radixsort_detail

#endif // !PSS_SRC_PARALLEL_BINGMANN_PARALLEL_RADIX_SORT_HEADER

/******************************************************************************/
