#                  ReactivePlusPlus library
#
#          Copyright Aleksey Loginov 2023 - present.
# Distributed under the Boost Software License, Version 1.0.
#    (See accompanying file LICENSE_1_0.txt or copy at
#          https://www.boost.org/LICENSE_1_0.txt)
#
# Project home: https://github.com/AlexInLog/ReactivePlusPlus
#

add_subdirectory(rpp)
add_subdirectory(extensions)

if (RPP_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

if(RPP_BUILD_TESTS)
    add_subdirectory(tests)
endif()

if(RPP_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
