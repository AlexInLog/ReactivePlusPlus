//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#include <doctest/doctest.h>

#include <rpp/observables.hpp>
#include <rpp/observers/mock_observer.hpp>
#include <rpp/operators/as_blocking.hpp>
#include <rpp/sources/create.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/never.hpp>
#include <rpp/subjects/replay_subject.hpp>

#include "rpp/disposables/fwd.hpp"
#include "rpp/operators/fwd.hpp"
#include "rpp/operators/subscribe.hpp"
#include "rpp/operators/take.hpp"

#include <chrono>
#include <thread>

TEST_CASE("create observable works properly as observable")
{
    size_t on_subscribe_called{};
    auto   observable = rpp::source::create<int>([&](auto&& observer) {
        ++on_subscribe_called;
        observer.on_next(1);
        observer.on_completed();
    });

    auto test = [&](auto&& observable) {
        SUBCASE("subscribe valid observer")
        {
            std::vector<int> on_next_vals{};
            size_t           on_error{};
            size_t           on_completed{};

            observable.subscribe([&](int v) { on_next_vals.push_back(v); },
                                 [&](const std::exception_ptr&) { ++on_error; },
                                 [&]() { ++on_completed; });

            CHECK(on_subscribe_called == 1u);
            CHECK(on_next_vals == std::vector{1});
            CHECK(on_error == 0u);
            CHECK(on_completed == 1u);
        }

        SUBCASE("subscribe disposed callbacks")
        {
            observable.subscribe(
                rpp::composite_disposable_wrapper::empty(),
                [](int) { },
                [](const std::exception_ptr&) { },
                []() { });

            CHECK(on_subscribe_called == 0u);
        }

        SUBCASE("subscribe disposed observer")
        {
            observable.subscribe(rpp::composite_disposable_wrapper::empty(), rpp::make_lambda_observer([](int) { }, [](const std::exception_ptr&) { }, []() { }));

            CHECK(on_subscribe_called == 0u);
        }

        SUBCASE("subscribe non-disposed callbacks")
        {
            observable.subscribe(
                rpp::composite_disposable_wrapper::make(),
                [](int) { },
                [](const std::exception_ptr&) { },
                []() { });

            CHECK(on_subscribe_called == 1u);
        }

        SUBCASE("subscribe non-disposed observer")
        {
            observable.subscribe(rpp::composite_disposable_wrapper::make(), rpp::make_lambda_observer([](int) { }, [](const std::exception_ptr&) { }, []() { }));

            CHECK(on_subscribe_called == 1u);
        }
    };

    SUBCASE("original observable")
    {
        test(observable);
    }

    SUBCASE("dynamic observable")
    {
        test(observable.as_dynamic());
    }

    SUBCASE("dynamic observable cast")
    {
        test(rpp::dynamic_observable<int>{observable});
    }

    SUBCASE("dynamic observable via move")
    {
        test(std::move(observable).as_dynamic()); // NOLINT
    }

    SUBCASE("dynamic observable cast via move")
    {
        test(rpp::dynamic_observable<int>{std::move(observable)}); // NOLINT
    }
}

TEST_CASE("blocking_observable blocks subscribe call")
{
    mock_observer_strategy<int> mock{};
    SUBCASE("on_completed inside observable")
    {
        rpp::source::create<int>([](auto&& observer) {
            std::thread(
                [observer = std::forward<decltype(observer)>(observer)] {
                    std::this_thread::sleep_for(std::chrono::milliseconds{100});
                    observer.on_completed();
                })
                .detach();
        })
            | rpp::operators::as_blocking()
            | rpp::operators::subscribe(mock);

        CHECK(mock.get_on_completed_count() == 1);
    }
    SUBCASE("on_error inside observable")
    {
        auto op  = rpp::operators::as_blocking();
        auto obs = rpp::source::create<int>([](auto&& observer) {
            std::thread(
                [observer = std::forward<decltype(observer)>(observer)] {
                    std::this_thread::sleep_for(std::chrono::milliseconds{100});
                    observer.on_error({});
                })
                .detach();
        });

        obs
            | op
            | rpp::operators::subscribe(mock);

        CHECK(mock.get_on_error_count() == 1);
    }
    SUBCASE("as_blocking + take(1)")
    {
        rpp::source::create<int>([](const auto& obs) {
            obs.on_next(1);
        })
            | rpp::ops::as_blocking()
            | rpp::ops::take(1)
            | rpp::operators::subscribe(mock);

        CHECK(mock.get_total_on_next_count() == 1);
        CHECK(mock.get_on_completed_count() == 1);
    }
}

TEST_CASE("base observables")
{
    mock_observer_strategy<int> mock{};

    SUBCASE("empty")
    {
        auto observable = rpp::source::empty<int>();
        SUBCASE("subscribe on this observable")
        {
            observable.subscribe(mock);
            SUBCASE("only on_completed called")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }
    SUBCASE("never")
    {
        auto observable = rpp::source::never<int>();
        SUBCASE("subscribe on this observable")
        {
            observable.subscribe(mock);
            SUBCASE("no any callbacks")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
    }
    SUBCASE("error")
    {
        auto observable = rpp::source::error<int>(std::make_exception_ptr(std::runtime_error{"MY EXCEPTION"}));
        SUBCASE("subscribe on this observable")
        {
            observable.subscribe(mock);
            SUBCASE("only on_error callback once")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 1);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
    }
}

TEST_CASE("pipe observable works properly as observable")
{
    SUBCASE("using const& variant")
    {
        auto observable = rpp::source::create<int>([](auto&& observer) {
            observer.on_next(1);
            observer.on_completed();
        });

        mock_observer_strategy<int> pipe_operator_observer{};
        mock_observer_strategy<int> pipe_function_observer{};

        observable | rpp::operators::subscribe(pipe_operator_observer);
        observable.pipe(rpp::operators::subscribe(pipe_function_observer));

        CHECK(pipe_operator_observer.get_total_on_next_count() == pipe_function_observer.get_total_on_next_count());
        CHECK(pipe_operator_observer.get_on_error_count() == pipe_function_observer.get_on_error_count());
        CHECK(pipe_operator_observer.get_on_completed_count() == pipe_function_observer.get_on_completed_count());
    }
    SUBCASE("using && variant")
    {
        mock_observer_strategy<int> pipe_operator_observer{};
        mock_observer_strategy<int> pipe_function_observer{};

        rpp::source::create<int>([](auto&& observer) {
            observer.on_next(1);
            observer.on_completed();
        }) | rpp::operators::subscribe(pipe_operator_observer);

        rpp::source::create<int>([](auto&& observer) {
            observer.on_next(1);
            observer.on_completed();
        }).pipe(rpp::operators::subscribe(pipe_function_observer));

        CHECK(pipe_operator_observer.get_total_on_next_count() == pipe_function_observer.get_total_on_next_count());
        CHECK(pipe_operator_observer.get_on_error_count() == pipe_function_observer.get_on_error_count());
        CHECK(pipe_operator_observer.get_on_completed_count() == pipe_function_observer.get_on_completed_count());
    }
}

TEST_CASE_TEMPLATE(
    "observable has type traits defined",
    TestType,
    rpp::empty_observable<int>,
    rpp::dynamic_observable<int>,
    rpp::blocking_observable<int, rpp::details::empty_strategy<int>>,
    rpp::connectable_observable<rpp::empty_observable<int>, rpp::subjects::replay_subject<int>>,
    rpp::grouped_observable<int, int, rpp::details::empty_strategy<int>>)
{
    SUBCASE("value_type defined")
    {
        CHECK(requires { typename TestType::value_type; });
        CHECK(std::is_same_v<typename TestType::value_type, int>);
    }
    SUBCASE("strategy_type defined")
    {
        CHECK(requires { typename TestType::strategy_type; });
    }
}
