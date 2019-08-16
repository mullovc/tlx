/*******************************************************************************
 * tests/sort_parallel_radixsort_test.cpp
 *
 * Integer sorting test program
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2014-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <functional>
#include <iostream>
#include <random>
#include <vector>
#include <cassert>

#include <tlx/die.hpp>
#include <tlx/logger.hpp>

#include <tlx/sort/parallel_radixsort.hpp>

// struct Something {
//     int a, b;

//     explicit Something(int x = 0)
//         : a(x), b(x * x)
//     { }

//     bool operator < (const Something& other) const {
//         return a < other.a;
//     }

//     friend std::ostream& operator << (std::ostream& os, const Something& s) {
//         return os << '(' << s.a << ',' << s.b << ')';
//     }
// };

void test_size(unsigned int size) {
    using namespace tlx::parallel_radixsort_detail;
    using Something = unsigned int;
    using key_type = uint8_t;

    std::cout << "testing parallel_radixsort with " << size << " items.\n";

    std::vector<Something> v(size);
    std::less<Something> cmp;

    std::mt19937 randgen(123456);
    std::uniform_int_distribution<unsigned int> distr;

    for (unsigned int i = 0; i < size; ++i)
        v[i] = Something(distr(randgen));

    radix_sort<std::vector<Something>::iterator, get_key<Something, key_type>>(
                v.begin(), v.end(), sizeof(Something) / sizeof(key_type));

    die_unless(std::is_sorted(v.cbegin(), v.cend(), cmp));
}

int main() {

    // run multiway mergesort tests for 0..256 sequences
    for (unsigned int i = 0; i < 256; ++i)
    {
        test_size(i);
    }

    // run multiway mergesort tests for 0..256 sequences
    for (unsigned int i = 256; i <= 16 * 1024 * 1024; i = 2 * i - i / 2)
    {
        test_size(i);
    }

    return 0;
}

/******************************************************************************/
