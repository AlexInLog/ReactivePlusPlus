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
#include <rpp/operators/take_while.hpp>
#include <rpp/sources/create.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/error.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"

#include <stdexcept>
#include <string>

TEST_CASE("take_while")
{
    auto mock = mock_observer_strategy<int>{};
    SUBCASE("-1-2-3-...")
    {
        auto obs = rpp::source::create<int>(
            [](const auto& sub) {
                int v{};
                while (!sub.is_disposed())
                    sub.on_next(v++);
            });

        SUBCASE("take while val <= 5")
        {
            obs | rpp::operators::take_while([](int val) { return val <= 5; }) | rpp::operators::subscribe(mock);

            CHECK(mock.get_received_values() == std::vector{0, 1, 2, 3, 4, 5});
        }

        SUBCASE("take while false")
        {
            auto op = rpp::operators::take_while([](auto) { return false; });
            obs | op | rpp::operators::subscribe(mock);

            CHECK(mock.get_received_values().empty());
        }
    }
    SUBCASE("-x")
    {
        rpp::source::error<int>({}) | rpp::operators::take_while([](int) { return false; }) | rpp::ops::subscribe(mock);
        CHECK(mock.get_received_values() == std::vector<int>{});
        CHECK(mock.get_on_error_count() == 1);
        CHECK(mock.get_on_completed_count() == 0);
    }

    SUBCASE("-|")
    {
        rpp::source::empty<int>() | rpp::operators::take_while([](int) { return true; }) | rpp::ops::subscribe(mock);
        CHECK(mock.get_received_values() == std::vector<int>{});
        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
    }
}

TEST_CASE("take_while doesn't produce extra copies")
{
    SUBCASE("take_while([](auto) { return true; })")
    {
        copy_count_tracker::test_operator(rpp::ops::take_while([](auto) { return true; }),
                                          {
                                              .send_by_copy = {.copy_count = 2, // 1 copy to lambda + 1 copy to subscriber
                                                               .move_count = 0},
                                              .send_by_move = {.copy_count = 1, // 1 copy to lambda
                                                               .move_count = 1} // 1 move to final subscriber
                                          });
    }
}

TEST_CASE("take_while satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::take_while([](auto) { return true; }));
}
