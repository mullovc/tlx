#ifndef SHADOW_SET_H
#define SHADOW_SET_H
#include <cstdint>
#include <utility>
#include <memory>
// #include <cstddef>

#include <tlx/meta/enable_if.hpp>

// template <typename Iterator_>
// class DataSet
// {
// public:
//     DataSet(Iterator_ begin, Iterator_ end)
//         : begin_(begin), end_(end)
//     { }

// protected:
//     Iterator_ begin_;
//     Iterator_ end_;

// public:
//     Iterator_ begin();
//     Iterator_ end();
// };

template <typename Iterator_>
class ShadowDataPtr
{
public:
    class DataSet {
    public:
        typedef Iterator_ Iterator;

        DataSet(Iterator_ begin, Iterator_ end)
            : begin_(begin), end_(end)
        { }

    protected:
        Iterator_ begin_;
        Iterator_ end_;

    public:
        Iterator_ begin();
        Iterator_ end();

        size_t size() const { return end_ - begin_; }
    };

protected:
    // Iterator_ active_begin;
    // Iterator_ active_end;
    // Iterator_ shadow_begin;
    // Iterator_ shadow_end;
    DataSet active_;
    DataSet shadow_;

    bool flipped_;
public:
    ShadowDataPtr(Iterator_ active_begin, Iterator_ active_end, 
              Iterator_ shadow_begin, Iterator_ shadow_end,
              bool flipped = false)
        : active_(active_begin, active_end),
          shadow_(shadow_begin, shadow_end),
          flipped_(flipped)
    { }

    // ShadowSet(Iterator_ active_begin_, Iterator_ active_end_, 
    //           Iterator_ shadow_begin_, Iterator_ shadow_end_,
    //           bool flipped = false)
    //     : active_begin(active_begin_),
    //       active_end(active_end_),
    //       shadow_begin(shadow_begin_),
    //       shadow_end(shadow_end_),
    //       flipped_(flipped)
    // { }

    virtual ~ShadowDataPtr();

    //! return currently active array
    const DataSet& active() const { return active_; }

    //! return current shadow array
    const DataSet& shadow() const { return shadow_; }

    //! true if flipped to back array
    bool flipped() const { return flipped_; }

    //! return valid length
    size_t size() const { return active_.size(); }

    //! Advance (both) pointers by given offset, return sub-array without flip
    ShadowDataPtr sub(size_t offset, size_t sub_size) const {
        assert(offset + sub_size <= size());
        return ShadowDataPtr(active_.subi(offset, offset + sub_size),
                        shadow_.subi(offset, offset + sub_size),
                        flipped_);
    }

    //! construct a ShadowDataPtr object specifying a sub-array with flipping
    //! to other array.
    ShadowDataPtr flip(size_t offset, size_t sub_size) const {
        assert(offset + sub_size <= size());
        return ShadowDataPtr(shadow_.subi(offset, offset + sub_size),
                         active_.subi(offset, offset + sub_size),
                         !flipped_);
    }

    //! return subarray pointer to n elements in original array, might copy from
    //! shadow before returning.
    ShadowDataPtr copy_back() const {
        if (!flipped_) {
            return *this;
        }
        else {
            std::move(active_.begin(), active_.end(), shadow_.begin());
            return ShadowDataPtr(shadow_, active_, !flipped_);
        }
    }
};


// template <typename Type, typename DataSet>
// inline
// typename std::enable_if<sizeof(Type) == 1, uint32_t>::type
// get_key(const DataSet& dset,
//         const typename DataSet::String& s, size_t depth) {
//     return dset.get_uint8(s, depth);
// }

// template <typename Type, typename DataSet>
// inline
// typename std::enable_if<sizeof(Type) == 4, uint32_t>::type
// get_key(const DataSet& dset,
//         const typename DataSet::String& s, size_t depth) {
//     return dset.get_uint32(s, depth);
// }

// template <typename Type, typename DataSet>
// inline
// typename std::enable_if<sizeof(Type) == 8, uint64_t>::type
// get_key(const DataSet& dset,
//         const typename DataSet::String& s, size_t depth) {
//     return dset.get_uint64(s, depth);
// }


#endif /* SHADOW_SET_H */
