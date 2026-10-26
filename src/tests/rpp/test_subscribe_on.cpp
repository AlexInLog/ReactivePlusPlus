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
#include <rpp/operators/subscribe_on.hpp>
#include <rpp/schedulers/current_thread.hpp>
#include <rpp/schedulers/new_thread.hpp>
#include <rpp/sources/create.hpp>
#include <rpp/sources/error.hpp>

#include "disposable_observable.hpp"
#include "rpp/disposables/composite_disposable.hpp"
#include "rpp/disposables/fwd.hpp"
#include "rpp/operators/fwd.hpp"
#include "rpp/operators/subscribe.hpp"
#include "rpp/schedulers/fwd.hpp"

#include <future>
#include <optional>
#include <thread>

TEST_CASE("subscribe_on schedules job in another scheduler")
{
    auto mock      = mock_observer_strategy<int>{};
    auto scheduler = rpp::schedulers::new_thread{};

    SUBCASE("observable")
    {
        std::promise<std::thread::id> thread_id{};
        auto                          obs = rpp::source::create<int>([&](auto&& sub) {
            thread_id.set_value(std::this_thread::get_id());
            sub.set_upstream(rpp::composite_disposable_wrapper::make());
            sub.on_next(1);
            sub.on_completed();
        });
        SUBCASE("subscribe on it with subscribe_on")
        {
            obs | rpp::ops::subscribe_on(scheduler) | rpp::ops::as_blocking() | rpp::ops::subscribe(mock);
            SUBCASE("expect to obtain value via scheduling")
            {
                REQUIRE(mock.get_total_on_next_count() == 1);
                REQUIRE(mock.get_on_completed_count() == 1);
                REQUIRE(thread_id.get_future().get() != std::this_thread::get_id());
            }
        }
    }
    SUBCASE("observable with error")
    {
        auto obs = rpp::source::error<int>(std::make_exception_ptr(std::runtime_error{""}));
        SUBCASE("subscribe on it with subscribe_on")
        {
            obs | rpp::ops::subscribe_on(scheduler) | rpp::ops::as_blocking() | rpp::ops::subscribe(mock);
            SUBCASE("expect to obtain error via scheduling")
            {
                REQUIRE(mock.get_total_on_next_count() == 0);
                REQUIRE(mock.get_on_error_count() == 1);
                REQUIRE(mock.get_on_completed_count() == 0);
            }
        }
    }
    SUBCASE("subscribe_on inside current_thread scheduler and disposing it before execution")
    {
        bool executed{};

        rpp::schedulers::current_thread::create_worker().schedule([&mock, &executed](const auto&) {
            auto d = rpp::composite_disposable_wrapper::make();
            rpp::source::create<int>([&](const auto&) {
                executed = true;
            })
                | rpp::ops::subscribe_on(rpp::schedulers::current_thread{})
                | rpp::ops::subscribe(mock.get_observer(d));

            CHECK(!executed);
            CHECK(!d.is_disposed());
            d.dispose();
            CHECK(!executed);
            CHECK(d.is_disposed());
            return rpp::schedulers::optional_delay_from_now{};
        },
                                                                  mock);

        CHECK(!executed);
    }

    SUBCASE("subscribe_on and then upstream updates upstream inside observer")
    {
        auto d      = rpp::composite_disposable_wrapper::make();
        auto second = rpp::composite_disposable_wrapper::make();
        rpp::source::create<int>([&](auto&& obs) {
            obs.set_upstream(rpp::disposable_wrapper{second});
        })
            | rpp::ops::subscribe_on(rpp::schedulers::current_thread{})
            | rpp::ops::subscribe(mock.get_observer(d));

        CHECK(!d.is_disposed());
        CHECK(!second.is_disposed());
        d.dispose();
        CHECK(d.is_disposed());
        CHECK(second.is_disposed());
    }
}


TEST_CASE("subscribe_on satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::subscribe_on(rpp::schedulers::current_thread{}));
}
