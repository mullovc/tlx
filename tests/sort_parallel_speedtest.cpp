/*******************************************************************************
 * tests/sort_parallel_speedtest.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2008-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>
#include <algorithm>
#include <random>

#include <tlx/sort/parallel_mergesort.hpp>
#include <tlx/sort/parallel_radixsort.hpp>

#include <tlx/die.hpp>
#include <tlx/timestamp.hpp>

// *** Settings

//! starting number of items to sort
const size_t min_items = 1024000;

//! maximum number of items to sort
const size_t max_items = 1024000l * 4096l;

//! minimum number of repeated sorts for each number of items
const size_t min_reps = 4;

//! minimum number of repeated sorts for each number of items
const size_t min_time = 4.0;

// -----------------------------------------------------------------------------

template <typename Container, typename key_type>
void run_tlx_radixsort(Container c) {
    using Iterator = typename Container::iterator;
    using T = typename std::iterator_traits<Iterator>::value_type;

    tlx::parallel_radixsort_detail::radix_sort<
        Iterator, tlx::parallel_radixsort_detail::get_key<T, key_type>>(
                c.begin(), c.end(), sizeof(T)/sizeof(key_type));
}

template <typename Container>
void run_tlx_radixsort_8(Container c) {
    run_tlx_radixsort<Container, uint8_t>(c);
}

template <typename Container>
void run_tlx_radixsort_16(Container c) {
    run_tlx_radixsort<Container, uint16_t>(c);
}

#if defined(_OPENMP)
template <typename Container>
void run_tlx_mergesort(Container c) {
    tlx::parallel_mergesort(c.begin(), c.end());
}
#endif // defined(_OPENMP)

template <typename Container>
void run_tlx_stdsort(Container c) {
    std::sort(c.begin(), c.end());
}

//! Repeat (short) tests until enough time elapsed and divide by the repeat.
template <typename Type, void (*SortFunc)(std::vector<Type>)>
void run_speedtest(size_t items, const std::string& algoname) {
    std::vector<Type> v(items);

    std::mt19937 randgen(123456);
    std::uniform_int_distribution<Type> distr;

    for (unsigned int i = 0; i < items; ++i)
        v[i] = Type(distr(randgen));

    size_t repeat = 0;
    double ts1, ts2, total_time = 0.0;

    do
    {
        std::shuffle(v.begin(), v.end(), randgen);

        ts1 = tlx::timestamp();

        // run timed test procedure
        SortFunc(v);

        ts2 = tlx::timestamp();

        total_time += ts2 - ts1;
        ++repeat;
    }
    while (total_time < min_time || repeat < min_reps);

    std::cout << "RESULT"
              << " algo=" << algoname
              << " items=" << items
              << " repeat=" << repeat
              << " time_total=" << total_time
              << " time="
              << std::fixed << std::setprecision(10) << (total_time / repeat)
              << std::endl;
}

//! Speed test them!
int main() {
    {
        for (size_t items = min_items; items <= max_items; items *= 2) {
            std::cout << "radixsort8: " << items << "\n";
            run_speedtest<uint64_t, run_tlx_radixsort_8>(items, "radixsort8");
        }
    }

    {
        for (size_t items = min_items; items <= max_items; items *= 2) {
            std::cout << "radixsort16: " << items << "\n";
            run_speedtest<uint64_t, run_tlx_radixsort_16>(items, "radixsort16");
        }
    }

#if defined(_OPENMP)
    {
        for (size_t items = min_items; items <= max_items; items *= 2) {
            std::cout << "mergesort: " << items << "\n";
            run_speedtest<uint64_t, run_tlx_mergesort>(items, "mergesort");
        }
    }
#endif // defined(_OPENMP)

    {
        for (size_t items = min_items; items <= max_items; items *= 2) {
            std::cout << "std::sort: " << items << "\n";
            run_speedtest<uint64_t, run_tlx_stdsort>(items, "std::sort");
        }
    }

    return 0;
}

/******************************************************************************/
