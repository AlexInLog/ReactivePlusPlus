//                  ReactivePlusPlus library
//
//          Copyright Aleksey Loginov 2023 - present.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          https://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/AlexInLog/ReactivePlusPlus
//

#include <doctest/doctest.h>

#include <rpp/observers/mock_observer.hpp>
#include <rpp/operators/tap.hpp>
#include <rpp/sources/concat.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/just.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"

TEST_CASE_TEMPLATE("tap observes emissions and doesn't modify them", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    auto mock = mock_observer_strategy<int>{};

    SUBCASE("observable with error emission")
    {
        auto obs =
            rpp::source::concat<TestType>(rpp::source::just<TestType>(1, 2, 3),
                                          rpp::source::error<int>(std::make_exception_ptr(std::runtime_error{""})));

        SUBCASE("subscribe")
        {
            size_t on_next_invoked  = 0;
            size_t on_error_invoked = 0;

            // clang-format off
            obs | rpp::ops::tap(
                [&](const int&) { ++on_next_invoked; },
                [&](const std::exception_ptr&) { ++on_error_invoked; })
                | rpp::ops::subscribe(mock);
            // clang-format on

            CHECK(mock.get_received_values() == std::vector{1, 2, 3});
            CHECK(mock.get_on_error_count() == 1);
            CHECK(mock.get_on_completed_count() == 0);

            CHECK(on_next_invoked == mock.get_total_on_next_count());
            CHECK(on_error_invoked == mock.get_on_error_count());
        }
    }

    SUBCASE("observable with completed emission")
    {
        auto obs = rpp::source::just<TestType>(1, 2, 3);

        SUBCASE("subscribe")
        {
            size_t on_next_invoked      = 0;
            size_t on_completed_invoked = 0;

            // clang-format off
            obs | rpp::ops::tap(
                [&](const int&) { ++on_next_invoked; },
                [&]() { ++on_completed_invoked; })
                | rpp::ops::subscribe(mock);
            // clang-format on

            CHECK(mock.get_received_values() == std::vector{1, 2, 3});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 1);

            CHECK(on_next_invoked == mock.get_total_on_next_count());
            CHECK(on_completed_invoked == mock.get_on_completed_count());
        }

        SUBCASE("pass on_next callback with auto argument")
        {
            size_t on_next_invoked = 0;

            obs | rpp::ops::tap([&](const auto&) { ++on_next_invoked; })
                | rpp::ops::subscribe(mock);

            CHECK(mock.get_received_values() == std::vector{1, 2, 3});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 1);

            CHECK(on_next_invoked == mock.get_total_on_next_count());
        }
    }
}

TEST_CASE("tap doesn't produce extra copies")
{
    // clang-format off
    copy_count_tracker::test_operator(rpp::ops::tap(),
                                      {
                                          .send_by_copy = { .copy_count = 1, // 1 copy on emission
                                                            .move_count = 0 },
                                          .send_by_move = { .copy_count = 0,
                                                            .move_count = 1 } // 1 move on emission
    });
    // clang-format on
}

TEST_CASE("tap satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::tap());
}
