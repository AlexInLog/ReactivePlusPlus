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
#include <rpp/schedulers/test_scheduler.hpp>
#include <rpp/sources/timer.hpp>

#include <chrono>

TEST_CASE("timer emit single value at provided duration")
{
    auto scheduler  = rpp::schedulers::test_scheduler{};
    auto scheduler2 = rpp::schedulers::test_scheduler{};
    auto mock       = mock_observer_strategy<size_t>{};
    auto mock2      = mock_observer_strategy<size_t>{};

    SUBCASE("timer observable")
    {
        auto when       = std::chrono::seconds{1};
        auto time_point = scheduler2.now() + when;
        auto obs        = rpp::source::timer(when, scheduler);
        auto obs2       = rpp::source::timer(time_point, scheduler2);

        SUBCASE("subscribe")
        {
            obs | rpp::ops::subscribe(mock);
            obs2 | rpp::ops::subscribe(mock2);

            SUBCASE("nothing happens immediately till scheduler advanced")
            {
                auto validate = [&](const auto& mock, const auto& scheduler) {
                    CHECK(mock.get_received_values() == std::vector<size_t>{});
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 0);

                    CHECK(scheduler.get_schedulings().size() == 1);
                    CHECK(scheduler.get_executions().empty());
                };
                validate(mock, scheduler);
                validate(mock2, scheduler2);
            }

            SUBCASE("advance time")
            {
                scheduler.time_advance(when);
                scheduler2.time_advance(when);

                SUBCASE("observer obtains value")
                {
                    auto validate = [&](const auto& mock) {
                        CHECK(mock.get_received_values() == std::vector<size_t>{0});
                        CHECK(mock.get_on_error_count() == 0);
                        CHECK(mock.get_on_completed_count() == 1);
                    };
                    validate(mock);
                    validate(mock2);
                }

                SUBCASE("timer schedules schedulable with provided interval")
                {
                    CHECK(scheduler.get_executions().size() == 1);
                    CHECK(scheduler2.get_executions().size() == 1);
                }
            }
        }
    }
}

TEST_CASE("timer emit single value at provided time_point")
{
    auto scheduler = rpp::schedulers::test_scheduler{};
    auto mock      = mock_observer_strategy<size_t>{};

    SUBCASE("timer observable")
    {
        auto when       = std::chrono::seconds{1};
        auto time_point = scheduler.now() + when;
        auto obs        = rpp::source::timer(time_point, scheduler);

        SUBCASE("subscribe")
        {
            scheduler.time_advance(when * 2);
            obs | rpp::ops::subscribe(mock);

            SUBCASE("expect value as time_point is in the past")
            {
                CHECK(mock.get_received_values() == std::vector<size_t>{0});
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }
}
