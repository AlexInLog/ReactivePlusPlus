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
#include <rpp/operators/take_last.hpp>
#include <rpp/sources/create.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/just.hpp>
#include <rpp/sources/never.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"


TEST_CASE("take_last sends last values in correct order on completed")
{
    auto mock = mock_observer_strategy<int>{};
    SUBCASE("observable of +-1-2-3-4-5-|")
    {
        auto obs = rpp::source::just(1, 2, 3, 4, 5);
        SUBCASE("subscribe on it via take_last(1)")
        {
            obs | rpp::ops::take_last(1) | rpp::ops::subscribe(mock);
            SUBCASE("see +-5-|")
            {
                CHECK(mock.get_received_values() == std::vector{5});
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }

        SUBCASE("subscribe on it via take_last(3)")
        {
            obs | rpp::ops::take_last(3) | rpp::ops::subscribe(mock);
            SUBCASE("see +-3-4-5-|")
            {
                CHECK(mock.get_received_values() == std::vector{3, 4, 5});
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }

        SUBCASE("subscribe on it via take_last(5)")
        {
            obs | rpp::ops::take_last(5) | rpp::ops::subscribe(mock);
            SUBCASE("see +-1-2-3-4-5-|")
            {
                CHECK(mock.get_received_values() == std::vector{1, 2, 3, 4, 5});
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }

        SUBCASE("subscribe on it via take_last(10)")
        {
            obs | rpp::ops::take_last(10) | rpp::ops::subscribe(mock);
            SUBCASE("see +-1-2-3-4-5-|")
            {
                CHECK(mock.get_received_values() == std::vector{1, 2, 3, 4, 5});
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
        SUBCASE("subscribe on it via take_last(0)")
        {
            obs | rpp::ops::take_last(0) | rpp::ops::subscribe(mock);
            SUBCASE("see +-|")
            {
                CHECK(mock.get_received_values().empty());
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }
}


TEST_CASE("take_last forwards error")
{
    auto mock = mock_observer_strategy<int>{};
    SUBCASE("observable of +-x")
    {
        auto source = rpp::source::error<int>(std::exception_ptr{});
        SUBCASE("subscribe on it via take(2)")
        {
            source | rpp::ops::take_last(2) | rpp::ops::subscribe(mock);
            SUBCASE("see error")
            {
                CHECK(mock.get_received_values().empty());
                CHECK(mock.get_on_error_count() == 1);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
    }

    SUBCASE("observable of +-1-x")
    {
        auto source = rpp::source::create<int>([](const auto& obs) {
            obs.on_next(1);
            obs.on_error({});
        });

        SUBCASE("subscribe on it via take(2)")
        {
            source | rpp::ops::take_last(2) | rpp::ops::subscribe(mock);
            SUBCASE("see error")
            {
                CHECK(mock.get_received_values().empty());
                CHECK(mock.get_on_error_count() == 1);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
    }
}

TEST_CASE("take_last completes with empty")
{
    auto mock = mock_observer_strategy<int>{};
    SUBCASE("observable of +-|")
    {
        auto source = rpp::source::empty<int>();
        SUBCASE("subscribe on it via take(2)")
        {
            source | rpp::ops::take_last(2) | rpp::ops::subscribe(mock);
            SUBCASE("see completed")
            {
                CHECK(mock.get_received_values().empty());
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }
}

TEST_CASE("take_last nothing with never")
{
    auto mock = mock_observer_strategy<int>{};
    SUBCASE("observable of +-|")
    {
        auto source = rpp::source::never<int>();
        SUBCASE("subscribe on it via take(2)")
        {
            source | rpp::ops::take_last(2) | rpp::ops::subscribe(mock);
            SUBCASE("see nothing")
            {
                CHECK(mock.get_received_values().empty());
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
    }
}

TEST_CASE("take_last doesn't produce extra copies")
{
    SUBCASE("take_last(1)")
    {
        copy_count_tracker::test_operator(rpp::ops::take_last(1),
                                          {
                                              .send_by_copy = {.copy_count = 1,  // 1 copy to internal state
                                                               .move_count = 1}, // 1 move to final subscriber
                                              .send_by_move = {.copy_count = 0,
                                                               .move_count = 2} // 1 move to internal state + 1 move to final subscriber
                                          });
    }
}

TEST_CASE("take_last satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::take_last(1));
}
