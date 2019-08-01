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

//! starting number of items to insert
const size_t min_items = 125;

//! maximum number of items to insert
// const size_t max_items = 1024000 * 128;
const size_t max_items = 1024000 * 64;

// -----------------------------------------------------------------------------

template <typename Container>
void run_tlx_radixsort(Container c) {
    using T = typename std::iterator_traits<
        typename Container::iterator>::value_type;
    tlx::parallel_radixsort_detail::radix_sort(c.begin(), c.end(), sizeof(T));
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

size_t repeat_until;

//! Repeat (short) tests until enough time elapsed and divide by the repeat.
template <typename Type, void (*SortFunc)(std::vector<Type>)>
void run_speedtest(size_t items, const std::string& algoname) {
    std::vector<Type> v(items);

    std::mt19937 randgen(123456);
    std::uniform_int_distribution<unsigned int> distr;

    for (unsigned int i = 0; i < items; ++i)
        v[i] = Type(distr(randgen));

    size_t repeat = 0;
    double ts1, ts2;

    do
    {
        // count repetition of timed tests
        repeat = 0;

        {
            // initialize test structures

            ts1 = tlx::timestamp();

            for (size_t r = 0; r <= repeat_until; r += items)
            {
                std::random_shuffle(v.begin(), v.end());

                // run timed test procedure
                SortFunc(v);
                ++repeat;
            }

            ts2 = tlx::timestamp();
        }

        std::cout << "Insert " << items << " repeat " << (repeat_until / items)
                  << " time " << (ts2 - ts1) << "\n";

        // discard and repeat if test took less than one second.
        if ((ts2 - ts1) < 1.0) repeat_until *= 2;
    }
    while ((ts2 - ts1) < 1.0);

    std::cout << "RESULT"
              << " algo=" << algoname
              << " items=" << items
              << " repeat=" << repeat
              << " time_total=" << (ts2 - ts1)
              << " time="
              << std::fixed << std::setprecision(10) << ((ts2 - ts1) / repeat)
              << std::endl;
}

//! Speed test them!
int main() {
    {
        repeat_until = min_items;

        for (size_t items = min_items; items <= max_items; items *= 2) {
            std::cout << "radixsort: " << items << "\n";
            run_speedtest<uint64_t, run_tlx_radixsort>(items, "radixsort");
        }
    }

#if defined(_OPENMP)
    {
        repeat_until = min_items;

        for (size_t items = min_items; items <= max_items; items *= 2) {
            std::cout << "mergesort: " << items << "\n";
            run_speedtest<uint64_t, run_tlx_mergesort>(items, "mergesort");
        }
    }
#endif // defined(_OPENMP)

    {
        repeat_until = min_items;

        for (size_t items = min_items; items <= max_items; items *= 2) {
            std::cout << "std::sort: " << items << "\n";
            run_speedtest<uint64_t, run_tlx_stdsort>(items, "std::sort");
        }
    }

    return 0;
}

/******************************************************************************/
