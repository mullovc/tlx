/*******************************************************************************
 * tests/string_test.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string.hpp>

#include <tlx/die.hpp>

//! Returns an initialized unsigned char[] array inside an std::string
#define ARRAY_AS_STRING(array) \
    std::string(reinterpret_cast<const char*>(array), sizeof(array))

void test_hexdump() {

    // take hex data and dump it into a string, then parse back into array
    const unsigned char hexdump[8] = {
        0x8D, 0xE2, 0x85, 0xD4, 0xBF, 0x98, 0xE6, 0x03
    };

    std::string hexdata = ARRAY_AS_STRING(hexdump);
    std::string hexstring = tlx::hexdump(hexdata);

    die_unequal(hexstring, "8DE285D4BF98E603");
    die_unequal(tlx::hexdump(hexdump, sizeof(hexdump)), "8DE285D4BF98E603");

    std::string hexparsed = tlx::parse_hexdump(hexstring);
    die_unequal(hexparsed, hexdata);

    // dump random binary string into hex and parse it back
    // std::string rand1 = tlx::random_binary(42);
    // die_unequal(tlx::parse_hexdump(tlx::hexdump(rand1)), rand1);

    // take the first hex list and dump it into source code format, then
    // compare it with correct data (which was also dumped with
    // hexdump_sourcecode())
    std::string hexsource = tlx::hexdump_sourcecode(hexdata, "abc");

    const unsigned char hexsourcecmp[68] = {
        0x63, 0x6F, 0x6E, 0x73, 0x74, 0x20, 0x75, 0x69,
        0x6E, 0x74, 0x38, 0x5F, 0x74, 0x20, 0x61, 0x62,
        0x63, 0x5B, 0x38, 0x5D, 0x20, 0x3D, 0x20, 0x7B,
        0x0A, 0x30, 0x78, 0x38, 0x44, 0x2C, 0x30, 0x78,
        0x45, 0x32, 0x2C, 0x30, 0x78, 0x38, 0x35, 0x2C,
        0x30, 0x78, 0x44, 0x34, 0x2C, 0x30, 0x78, 0x42,
        0x46, 0x2C, 0x30, 0x78, 0x39, 0x38, 0x2C, 0x30,
        0x78, 0x45, 0x36, 0x2C, 0x30, 0x78, 0x30, 0x33,
        0x0A, 0x7D, 0x3B, 0x0A
    };

    die_unequal(hexsource, ARRAY_AS_STRING(hexsourcecmp));

    // test parse_hexdump with illegal strings
    die_noexcept(tlx::parse_hexdump("illegal"), std::runtime_error);
    die_noexcept(tlx::parse_hexdump("8DE285D4BF98E60"), std::runtime_error);
}

void test_starts_with_ends_with() {

    die_unless(tlx::starts_with("abcdef", "abc"));
    die_unless(!tlx::starts_with("abcdef", "def"));
    die_unless(tlx::ends_with("abcdef", "def"));
    die_unless(!tlx::ends_with("abcdef", "abc"));

    die_unless(!tlx::starts_with("abcdef", "ABC"));

    die_unless(tlx::starts_with_icase("abcdef", "ABC"));
    die_unless(!tlx::starts_with_icase("abcdef", "DEF"));
    die_unless(tlx::ends_with_icase("abcdef", "DEF"));
    die_unless(!tlx::ends_with_icase("abcdef", "ABC"));

    die_unless(tlx::starts_with("abcdef", ""));
    die_unless(tlx::ends_with("abcdef", ""));

    die_unless(!tlx::starts_with("", "abc"));
    die_unless(!tlx::ends_with("", "abc"));

    die_unless(tlx::starts_with("", ""));
    die_unless(tlx::ends_with("", ""));
}

int main() {

    test_hexdump();
    test_starts_with_ends_with();

    return 0;
}

/******************************************************************************/
