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
#include <rpp/operators/skip.hpp>
#include <rpp/schedulers/current_thread.hpp>
#include <rpp/schedulers/immediate.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/just.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"


TEST_CASE_TEMPLATE("skip ignores first `count` of items",
                   TestType,
                   std::pair<rpp::schedulers::current_thread, rpp::memory_model::use_stack>,
                   std::pair<rpp::schedulers::immediate, rpp::memory_model::use_stack>,
                   std::pair<rpp::schedulers::current_thread, rpp::memory_model::use_shared>,
                   std::pair<rpp::schedulers::immediate, rpp::memory_model::use_shared>)
{
    using memory_model = std::tuple_element_t<1, TestType>;
    using scheduler    = std::tuple_element_t<0, TestType>;

    auto mock = mock_observer_strategy<int>{};
    auto obs  = rpp::source::just<memory_model>(scheduler{}, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    SUBCASE("subscribe to observable of 10 items via skip(5) skips first 5 items")
    {
        constexpr size_t count   = 5;
        auto             new_obs = obs | rpp::operators::skip(count);
        new_obs.subscribe(mock);

        CHECK(mock.get_received_values() == std::vector{5, 6, 7, 8, 9});
        CHECK(mock.get_on_completed_count() == 1);

        SUBCASE("second subscription sees same")
        {
            auto mock_2 = mock_observer_strategy<int>{};
            new_obs.subscribe(mock_2);

            CHECK(mock_2.get_received_values() == std::vector{5, 6, 7, 8, 9});
            CHECK(mock.get_on_completed_count() == 1);
        }
    }
    SUBCASE("subscribe to observable of 10 via skip(0) emits all values")
    {
        constexpr size_t count   = 0;
        auto             new_obs = obs | rpp::ops::skip(count);
        new_obs.subscribe(mock);
        CHECK(mock.get_received_values() == std::vector{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        CHECK(mock.get_on_completed_count() == 1);
    }
    SUBCASE("subscribe to observable of 10 via skip(1000) emits nothing but completes")
    {
        constexpr size_t count   = 1000;
        auto             new_obs = obs | rpp::ops::skip(count);
        new_obs.subscribe(mock);
        CHECK(mock.get_received_values() == std::vector<int>{});
        CHECK(mock.get_on_completed_count() == 1);
    }
}

TEST_CASE("skip forwards error")
{
    auto mock = mock_observer_strategy<int>{};

    rpp::source::error<int>({}) | rpp::operators::skip(1) | rpp::ops::subscribe(mock);
    CHECK(mock.get_received_values() == std::vector<int>{});
    CHECK(mock.get_on_error_count() == 1);
    CHECK(mock.get_on_completed_count() == 0);
}

TEST_CASE("skip forwards completion")
{
    auto mock = mock_observer_strategy<int>{};
    rpp::source::empty<int>() | rpp::operators::skip(1) | rpp::ops::subscribe(mock);
    CHECK(mock.get_received_values() == std::vector<int>{});
    CHECK(mock.get_on_error_count() == 0);
    CHECK(mock.get_on_completed_count() == 1);
}

TEST_CASE("skip doesn't produce extra copies")
{
    SUBCASE("skip(1)")
    {
        copy_count_tracker::test_operator(rpp::ops::skip(1),
                                          {
                                              .send_by_copy = {.copy_count = 1, // 1 copy to final subscriber
                                                               .move_count = 0},
                                              .send_by_move = {.copy_count = 0,
                                                               .move_count = 1} // 1 move to final subscriber
                                          },
                                          2);
    }
}

TEST_CASE("skip satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::skip(1));
}
