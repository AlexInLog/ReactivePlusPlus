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
#include <rpp/operators/start_with.hpp>

TEST_CASE("start_with works as concat with prepending instead of adding at the end")
{
    auto mock = mock_observer_strategy<int>{};

    auto check = [&] {
        SUBCASE("obtain values from start_with firstly, then from original observable")
        {
            CHECK(mock.get_received_values() == std::vector{2, 3, 1});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 1);
        }
    };
    SUBCASE("3 observables")
    {
        auto obs_1 = rpp::source::just(1);
        auto obs_2 = rpp::source::just(2);
        auto obs_3 = rpp::source::just(3);
        SUBCASE("subscribe on them via start_with")
        {
            obs_1 | rpp::ops::start_with(obs_2, obs_3) | rpp::ops::subscribe(mock);

            check();
        }
    }

    SUBCASE("observable")
    {
        auto obs_1 = rpp::source::just(1);
        SUBCASE("subscribe on it via start_with with values")
        {
            obs_1 | rpp::ops::start_with(2, 3) | rpp::ops::subscribe(mock);

            check();
        }
        SUBCASE("subscribe on it via start_with<shared> with values")
        {
            obs_1 | rpp::ops::start_with<rpp::memory_model::use_shared>(2, 3) | rpp::ops::subscribe(mock);

            check();
        }
    }
}
