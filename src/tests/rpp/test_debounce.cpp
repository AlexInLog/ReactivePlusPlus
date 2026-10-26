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
#include <rpp/operators/debounce.hpp>
#include <rpp/schedulers/test_scheduler.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/subjects/publish_subject.hpp>

#include "disposable_observable.hpp"


TEST_CASE("debounce emit only items where timeout reached")
{
    auto                            debounce_delay = std::chrono::seconds{2};
    rpp::schedulers::test_scheduler scheduler{};
    auto                            start = rpp::schedulers::test_scheduler::s_current_time;

    SUBCASE("subject of items and subscriber subscribed on it via debounce")
    {
        auto                                               mock = mock_observer_strategy<int>{};
        std::optional<rpp::subjects::publish_subject<int>> optional_subj{rpp::subjects::publish_subject<int>{}};
        auto&                                              subj = optional_subj.value();
        subj.get_observable() | rpp::ops::debounce(debounce_delay, scheduler) | rpp::ops::subscribe(mock);
        SUBCASE("emit value")
        {
            subj.get_observer().on_next(1);
            SUBCASE("delay scheduled action to track period")
            {
                CHECK(scheduler.get_schedulings() == std::vector{start + debounce_delay});
                CHECK(scheduler.get_executions().empty());
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
            }
            SUBCASE("scheduler reached delayed time")
            {
                scheduler.time_advance(debounce_delay);
                SUBCASE("emission reached mock")
                {
                    CHECK(scheduler.get_schedulings() == std::vector{start + debounce_delay});
                    CHECK(scheduler.get_executions() == std::vector{start + debounce_delay});
                    CHECK(mock.get_received_values() == std::vector{1});
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 0);
                }
            }
            SUBCASE("emit on completed")
            {
                subj.get_observer().on_completed();
                SUBCASE("emission reached mock with on completed without schedulable exection")
                {
                    CHECK(scheduler.get_schedulings() == std::vector{start + debounce_delay});
                    CHECK(scheduler.get_executions().empty());
                    CHECK(mock.get_received_values() == std::vector{1});
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 1);
                }
            }
            SUBCASE("new value emitted before scheduler reached requested time")
            {
                scheduler.time_advance(debounce_delay / 2);
                subj.get_observer().on_next(2);
                SUBCASE("nothing changed immediately")
                {
                    CHECK(scheduler.get_schedulings() == std::vector{start + debounce_delay});
                    CHECK(scheduler.get_executions().empty());
                    CHECK(mock.get_total_on_next_count() == 0);
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 0);
                }
                SUBCASE("scheduler reached originally requested time")
                {
                    scheduler.time_advance(debounce_delay / 2);
                    SUBCASE("delay re-schedule schedulable to new delay timepoint")
                    {
                        CHECK(scheduler.get_schedulings() == std::vector{start + debounce_delay, start + debounce_delay / 2 + debounce_delay});
                        CHECK(scheduler.get_executions() == std::vector{start + debounce_delay});
                        CHECK(mock.get_total_on_next_count() == 0);
                        CHECK(mock.get_on_error_count() == 0);
                        CHECK(mock.get_on_completed_count() == 0);
                    }
                    SUBCASE("scheduler reached delayed time")
                    {
                        scheduler.time_advance(debounce_delay / 2);
                        SUBCASE("emission reached mock")
                        {
                            CHECK(scheduler.get_schedulings() == std::vector{start + debounce_delay, start + debounce_delay / 2 + debounce_delay});
                            CHECK(scheduler.get_executions() == std::vector{start + debounce_delay, start + debounce_delay / 2 + debounce_delay});
                            CHECK(mock.get_received_values() == std::vector{2});
                            CHECK(mock.get_on_error_count() == 0);
                            CHECK(mock.get_on_completed_count() == 0);
                        }
                    }
                }
            }
            SUBCASE("subject destoryed and then schedulable reaches schedulable")
            {
                optional_subj.reset();
                scheduler.time_advance(debounce_delay);
                SUBCASE("emission reached mock")
                {
                    CHECK(scheduler.get_schedulings() == std::vector{start + debounce_delay});
                    CHECK(scheduler.get_executions() == std::vector{start + debounce_delay});
                    CHECK(mock.get_received_values() == std::vector{1});
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 0);
                }
            }
        }
    }
}


TEST_CASE("debounce is not deadlocking is_disposed")
{
    std::optional<rpp::dynamic_observer<int>> observer{};

    rpp::schedulers::test_scheduler scheduler{};

    rpp::source::create<int>([&observer](auto&& obs) {
        observer = std::forward<decltype(obs)>(obs).as_dynamic();
        observer->on_next(1);
    })
        | rpp::operators::debounce(std::chrono::seconds{1}, scheduler)
        | rpp::ops::subscribe([&observer](int) {
              CHECK(observer);
              CHECK(!observer->is_disposed());
          });
    scheduler.time_advance(std::chrono::seconds{1});
}


TEST_CASE("debounce forwards error")
{
    auto mock = mock_observer_strategy<int>{};

    rpp::source::error<int>({}) | rpp::operators::debounce({}, rpp::schedulers::immediate{}) | rpp::ops::subscribe(mock);
    CHECK(mock.get_received_values() == std::vector<int>{});
    CHECK(mock.get_on_error_count() == 1);
    CHECK(mock.get_on_completed_count() == 0);
}


TEST_CASE("debounce satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::debounce(std::chrono::seconds{1}, rpp::schedulers::test_scheduler{}));
}
