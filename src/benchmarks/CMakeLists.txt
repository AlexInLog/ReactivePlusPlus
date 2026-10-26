#                  ReactivePlusPlus library
#
#          Copyright Aleksey Loginov 2023 - present.
# Distributed under the Boost Software License, Version 1.0.
#    (See accompanying file LICENSE_1_0.txt or copy at
#          https://www.boost.org/LICENSE_1_0.txt)
#
# Project home: https://github.com/AlexInLog/ReactivePlusPlus
#

set(TARGET benchmarks)

add_executable(${TARGET} benchmarks.cpp)

target_link_libraries(${TARGET} PRIVATE rpp nanobench::nanobench)
if (RPP_BUILD_RXCPP)
    target_link_libraries(${TARGET} PRIVATE rxcpp)
    target_compile_definitions(${TARGET} PRIVATE RPP_BUILD_RXCPP)

    get_target_property(DEP_DIR rxcpp INTERFACE_INCLUDE_DIRECTORIES)
    target_include_directories(${TARGET} SYSTEM PRIVATE ${DEP_DIR})
endif()
if(RPP_DISABLE_DISPOSABLES_OPTIMIZATION)
    target_compile_definitions(${TARGET} PRIVATE "RPP_DISABLE_DISPOSABLES_OPTIMIZATION=${RPP_DISABLE_DISPOSABLES_OPTIMIZATION}")
endif()

set_target_properties(${TARGET} PROPERTIES FOLDER Tests)
set_target_properties(${TARGET} PROPERTIES CXX_CLANG_TIDY "")


add_test(NAME ${TARGET} COMMAND  $<TARGET_FILE:${TARGET}> --dump=${RPP_TEST_RESULTS_DIR}/benchmarks_results.json)
