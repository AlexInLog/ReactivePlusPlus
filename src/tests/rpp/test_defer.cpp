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
#include <rpp/sources/create.hpp>
#include <rpp/sources/defer.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/from.hpp>
#include <rpp/sources/never.hpp>

TEST_CASE("defer on different sources")
{
    auto mock = mock_observer_strategy<int>{};

    SUBCASE("just")
    {
        auto obs = rpp::source::defer([] { return rpp::source::just(1); });
        obs.subscribe(mock);

        CHECK(mock.get_received_values() == std::vector{1});
        CHECK(mock.get_total_on_next_count() == 1);
        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
    }
    SUBCASE("error")
    {
        auto obs = rpp::source::defer([] { return rpp::source::error<int>(std::make_exception_ptr(std::runtime_error{"error"})); });
        obs.subscribe(mock);

        CHECK(mock.get_total_on_next_count() == 0);
        CHECK(mock.get_on_error_count() == 1);
        CHECK(mock.get_on_completed_count() == 0);
    }
    SUBCASE("empty")
    {
        auto obs = rpp::source::defer([] { return rpp::source::empty<int>(); });
        obs.subscribe(mock);

        CHECK(mock.get_total_on_next_count() == 0);
        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
    }
    SUBCASE("never")
    {
        auto obs = rpp::source::defer([] { return rpp::source::never<int>(); });
        obs.subscribe(mock);

        CHECK(mock.get_total_on_next_count() == 0);
        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 0);
    }
}

TEST_CASE("defer on mutable sources")
{

    auto obs = rpp::source::defer([] {
        const auto state     = std::make_shared<int>(0);
        auto       inner_obs = rpp::source::create<int>([state](const auto& obs) {
            obs.on_next((*state)++);
        });
        return inner_obs;
    });

    SUBCASE("defer returns same value on multiple subscriptions")
    {
        auto mock1 = mock_observer_strategy<int>{};
        auto mock2 = mock_observer_strategy<int>{};
        obs.subscribe(mock1);
        obs.subscribe(mock2);

        CHECK(mock1.get_received_values() == std::vector{0});
        CHECK(mock2.get_received_values() == std::vector{0});
    }
}
