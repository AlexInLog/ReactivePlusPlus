//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

// MSVC has bad support for the empty base classes, but provides specificator to enable full support. GCC/Clang works as expected
#if defined(_MSC_VER) && _MSC_FULL_VER >= 190023918
    #define RPP_EMPTY_BASES __declspec(empty_bases)
#else
    #define RPP_EMPTY_BASES
#endif

#if defined(__APPLE__) && defined(__clang__) // apple works unexpectedly bad on clang 15 =C
    #define RPP_NO_UNIQUE_ADDRESS
#else
    #define RPP_NO_UNIQUE_ADDRESS [[no_unique_address]]
#endif
