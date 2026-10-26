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
#include <rpp/operators/last.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/just.hpp>
#include <rpp/sources/never.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"


TEST_CASE("last only emits once")
{
    auto mock = mock_observer_strategy<int>{};

    SUBCASE("observable of -1-| - shall see -1-|")
    {
        rpp::source::just(1)
            | rpp::ops::last()
            | rpp::ops::subscribe(mock);

        CHECK(mock.get_received_values() == std::vector{1});
        CHECK(mock.get_on_completed_count() == 1);
        CHECK(mock.get_on_error_count() == 0);
    }

    SUBCASE("observable of -1-2-3-| - shall see -3-|")
    {
        rpp::source::just(1, 2, 3)
            | rpp::ops::last()
            | rpp::ops::subscribe(mock);

        CHECK(mock.get_received_values() == std::vector{3});
        CHECK(mock.get_on_completed_count() == 1);
        CHECK(mock.get_on_error_count() == 0);
    }

    SUBCASE("observable of never - shall not see neither completed nor error event")
    {
        rpp::source::never<int>()
            | rpp::ops::last()
            | rpp::ops::subscribe(mock);

        CHECK(mock.get_received_values().empty());
        CHECK(mock.get_on_completed_count() == 0);
        CHECK(mock.get_on_error_count() == 0);
    }

    SUBCASE("observable of x-| - shall see error and no-completed event")
    {
        rpp::source::error<int>(std::make_exception_ptr(std::runtime_error{""}))
            | rpp::ops::last()
            | rpp::ops::subscribe(mock);

        CHECK(mock.get_received_values().empty());
        CHECK(mock.get_on_completed_count() == 0);
        CHECK(mock.get_on_error_count() == 1);
    }

    SUBCASE("observable of ---| - shall see -x")
    {
        rpp::source::empty<int>()
            | rpp::ops::last()
            | rpp::ops::subscribe(mock);

        CHECK(mock.get_received_values().empty());
        CHECK(mock.get_on_completed_count() == 0);
        CHECK(mock.get_on_error_count() == 1);
    }
}

TEST_CASE("last doesn't produce extra copies")
{
    SUBCASE("last()")
    {
        copy_count_tracker::test_operator(rpp::ops::last(),
                                          {
                                              .send_by_copy = {.copy_count = 2,  // 2 copy to std::optional
                                                               .move_count = 1}, // 1 move to final subscriber
                                              .send_by_move = {.copy_count = 0,
                                                               .move_count = 3} // 2 move to std::optional + 1 move to final subscriber
                                          },
                                          2);
    }
}

TEST_CASE("last satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::last());
}
