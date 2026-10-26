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
#include <rpp/operators/as_blocking.hpp>
#include <rpp/operators/subscribe.hpp>
#include <rpp/operators/take.hpp>
#include <rpp/schedulers/current_thread.hpp>
#include <rpp/schedulers/immediate.hpp>
#include <rpp/schedulers/new_thread.hpp>
#include <rpp/schedulers/test_scheduler.hpp>
#include <rpp/sources/interval.hpp>

#include <chrono>

TEST_CASE("interval emit values with provided interval")
{
    auto scheduler = rpp::schedulers::test_scheduler{};
    auto mock      = mock_observer_strategy<size_t>{};

    SUBCASE("interval observable")
    {
        auto interval     = std::chrono::seconds{1};
        auto obs          = rpp::source::interval(interval, scheduler);
        auto initial_time = rpp::schedulers::test_scheduler::worker_strategy::now();

        SUBCASE("subscribe on it via take 3")
        {
            obs | rpp::ops::take(3) | rpp::ops::subscribe(mock);

            SUBCASE("nothing happens immediately till scheduler advanced")
            {
                CHECK(mock.get_received_values() == std::vector<size_t>{});
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
                CHECK(scheduler.get_schedulings() == std::vector{initial_time + interval});
                CHECK(scheduler.get_executions().empty());
            }

            SUBCASE("move time in advance on interval once")
            {
                scheduler.time_advance(interval);

                SUBCASE("observer obtains first value")
                {
                    CHECK(mock.get_received_values() == std::vector<size_t>{0});
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 0);
                }

                SUBCASE("interval schedules schedulable with provided interval")
                {
                    CHECK(scheduler.get_schedulings() == std::vector{initial_time + interval, initial_time + 2 * interval});
                    CHECK(scheduler.get_executions() == std::vector{initial_time + interval});
                }
            }

            SUBCASE("move time in advance on interval enough amount of time")
            {
                for (size_t i = 0; i < 5; ++i)
                    scheduler.time_advance(interval);

                SUBCASE("observer obtains sequence of values")
                {
                    CHECK(mock.get_received_values() == std::vector<size_t>{0, 1, 2});
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 1);
                }

                SUBCASE("interval schedules schedulable with provided interval")
                {
                    CHECK(scheduler.get_executions() == std::vector{initial_time + interval, initial_time + 2 * interval, initial_time + 3 * interval});
                    CHECK(scheduler.get_schedulings() == std::vector{initial_time + interval, initial_time + 2 * interval, initial_time + 3 * interval});
                }
            }
        }
    }

    SUBCASE("interval observable with initial delay duration")
    {
        auto initial_delay = std::chrono::seconds{2};
        auto interval      = std::chrono::seconds{1};
        auto obs           = rpp::source::interval(initial_delay, interval, scheduler);
        auto initial_time  = rpp::schedulers::test_scheduler::worker_strategy::now();

        SUBCASE("subscribe on it via take 3")
        {
            obs | rpp::ops::take(3) | rpp::ops::subscribe(mock);

            SUBCASE("nothing happens immediately till scheduler advanced")
            {
                CHECK(mock.get_received_values() == std::vector<size_t>{});
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
                CHECK(scheduler.get_schedulings() == std::vector{initial_time + initial_delay});
                CHECK(scheduler.get_executions().empty());
            }

            SUBCASE("move time in advance on interval enough amount of time")
            {
                for (size_t i = 0; i < 5; ++i)
                    scheduler.time_advance(interval);
                scheduler.time_advance(interval);

                SUBCASE("observer obtains sequence of values")
                {
                    CHECK(mock.get_received_values() == std::vector<size_t>{0, 1, 2});
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 1);
                }

                SUBCASE("interval schedules schedulable with provided interval")
                {
                    CHECK(scheduler.get_schedulings() == std::vector{initial_time + initial_delay, initial_time + initial_delay + interval, initial_time + initial_delay + 2 * interval});
                    CHECK(scheduler.get_executions() == std::vector{initial_time + initial_delay, initial_time + initial_delay + interval, initial_time + initial_delay + 2 * interval});
                }
            }
        }
    }

    SUBCASE("interval observable with initial delay time_point")
    {
        auto when          = std::chrono::seconds{2};
        auto initial_delay = scheduler.now() + when;
        auto interval      = std::chrono::seconds{1};
        auto obs           = rpp::source::interval(initial_delay, interval, scheduler);

        SUBCASE("subscribe")
        {
            scheduler.time_advance(when * 2);
            obs | rpp::ops::take(1) | rpp::ops::subscribe(mock);

            SUBCASE("expect value as initial_delay time_point is in the past")
            {
                CHECK(mock.get_received_values() == std::vector<size_t>{0});
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }
}
