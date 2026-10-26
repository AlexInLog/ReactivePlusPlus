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
#include <rpp/operators/map.hpp>
#include <rpp/operators/throttle.hpp>
#include <rpp/schedulers/test_scheduler.hpp>
#include <rpp/subjects/publish_subject.hpp>

#include "disposable_observable.hpp"

TEST_CASE("throttle throttles emissions")
{
    auto       mock              = mock_observer_strategy<std::tuple<int, rpp::schedulers::time_point>>{};
    auto       subj              = rpp::subjects::publish_subject<int>{};
    const auto throttle_duration = std::chrono::seconds{2};
    subj.get_observable() | rpp::ops::throttle<rpp::schedulers::test_scheduler>(throttle_duration) | rpp::ops::map([](int v) { return std::tuple{v, rpp::schedulers::test_scheduler::now()}; }) | rpp::ops::subscribe(mock);
    SUBCASE("emiting second value forwards it immediately")
    {
        const auto first_value_time = rpp::schedulers::test_scheduler::now();
        subj.get_observer().on_next(1);
        CHECK(mock.get_received_values() == std::vector{std::tuple{1, first_value_time}});
        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 0);
        SUBCASE("emitting second value in throttle_duration/2 not forwards it")
        {
            rpp::schedulers::test_scheduler{}.time_advance(throttle_duration / 2);

            subj.get_observer().on_next(2);
            CHECK(mock.get_received_values() == std::vector{std::tuple{1, first_value_time}});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 0);
            SUBCASE("emitting third value in throttle_duration/2+throttle_duration/2 forwards it")
            {
                rpp::schedulers::test_scheduler{}.time_advance(throttle_duration / 2);

                subj.get_observer().on_next(3);
                CHECK(mock.get_received_values() == std::vector{std::tuple{1, first_value_time}, std::tuple{3, rpp::schedulers::test_scheduler::now()}});
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
        SUBCASE("emitting error forwards it immediately")
        {
            subj.get_observer().on_error({});
            CHECK(mock.get_received_values() == std::vector{std::tuple{1, first_value_time}});
            CHECK(mock.get_on_error_count() == 1);
            CHECK(mock.get_on_completed_count() == 0);
        }
        SUBCASE("emitting completed forwards it immediately")
        {
            subj.get_observer().on_completed();
            CHECK(mock.get_received_values() == std::vector{std::tuple{1, first_value_time}});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 1);
        }
        SUBCASE("emitting second value in throttle_duration forwards it")
        {
            rpp::schedulers::test_scheduler{}.time_advance(throttle_duration);

            subj.get_observer().on_next(2);
            CHECK(mock.get_received_values() == std::vector{std::tuple{1, first_value_time}, std::tuple{2, rpp::schedulers::test_scheduler::now()}});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 0);
        }
        SUBCASE("emitting second value in 3/2*throttle_duration forwards it")
        {
            rpp::schedulers::test_scheduler{}.time_advance(throttle_duration / 2 * 3);

            subj.get_observer().on_next(2);
            CHECK(mock.get_received_values() == std::vector{std::tuple{1, first_value_time}, std::tuple{2, rpp::schedulers::test_scheduler::now()}});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 0);
        }
    }
}


TEST_CASE("throttle satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::throttle(std::chrono::seconds{1}));
}
