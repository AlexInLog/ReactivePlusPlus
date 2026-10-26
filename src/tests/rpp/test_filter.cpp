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
#include <rpp/operators/filter.hpp>
#include <rpp/sources/just.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"

#include <stdexcept>
#include <string>

TEST_CASE_TEMPLATE("filter", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    mock_observer_strategy<int> mock{};

    auto obs = rpp::source::just<TestType>(1, 2, 3, 4);

    SUBCASE("filter emits only satisfying values")
    {
        auto filter = rpp::operators::filter([](auto v) { return v % 2 == 0; });
        obs | filter | rpp::operators::subscribe(mock);

        CHECK(mock.get_received_values() == std::vector{2, 4});
        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
    }


    SUBCASE("filter with exception value")
    {
        obs | rpp::operators::filter([](int) -> bool { throw std::runtime_error{""}; }) | rpp::operators::subscribe(mock); // NOLINT

        CHECK(mock.get_received_values() == std::vector<int>{});
        CHECK(mock.get_on_error_count() == 1);
        CHECK(mock.get_on_completed_count() == 0);
    }
}

TEST_CASE("filter doesn't produce extra copies")
{
    SUBCASE("filter([](copy_count_tracker){return true;})")
    {
        copy_count_tracker::test_operator(rpp::ops::filter([](copy_count_tracker) { return true; }), // NOLINT
                                          {
                                              .send_by_copy = {.copy_count = 2, // 1 copy to filter lambda + 1 move to subscriber
                                                               .move_count = 0},
                                              .send_by_move = {.copy_count = 1, // 1 copy to filter lambda
                                                               .move_count = 1} // 1 move to final subscriber
                                          });
    }

    SUBCASE("filter([](const copy_count_tracker&){return false;})")
    {
        copy_count_tracker::test_operator(rpp::ops::filter([](const copy_count_tracker&) { return false; }),
                                          {.send_by_copy = {.copy_count = 0,
                                                            .move_count = 0},
                                           .send_by_move = {.copy_count = 0,
                                                            .move_count = 0}});
    }
}

TEST_CASE("filter satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::filter([](const int&) { return false; }));
}
