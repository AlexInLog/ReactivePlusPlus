//                  ReactivePlusPlus library
//
//          Copyright Aleksey Loginov 2023 - present.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          https://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/AlexInLog/ReactivePlusPlus
//

#pragma once

#include <doctest/doctest.h>

#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/observers/dynamic_observer.hpp>
#include <rpp/sources/create.hpp>

template<typename T>
auto observable_with_disposable(rpp::disposable_wrapper d)
{
    return rpp::source::create<T>([d](auto&& obs) {
        obs.set_upstream(d);
    });
}

template<rpp::constraint::decayed_type Type, rpp::details::observables::constraint::disposables_strategy Strategy>
struct wrapped_observable_strategy_set_upstream
{
    using value_type                   = Type;
    using optimal_disposables_strategy = Strategy;

    auto subscribe(auto&& observer) const
    {
        observer.set_upstream(rpp::composite_disposable_wrapper::make());
    }
};

template<rpp::constraint::decayed_type Type, rpp::details::observables::constraint::disposables_strategy Strategy>
struct wrapped_observable_strategy_no_set_upstream
{
    using value_type                   = Type;
    using optimal_disposables_strategy = Strategy;

    auto subscribe(auto&&) const { }
};

template<typename T>
void test_operator_over_observable_finish_before_dispose(auto&& op)
{
    SUBCASE("operator calls on_error before dispose")
    {
        bool callback_called       = false;
        auto observable_disposable = rpp::make_callback_disposable([&callback_called]() noexcept {
            callback_called = true;
        });
        auto observable            = rpp::source::create<T>([&observable_disposable](auto&& obs) {
            obs.set_upstream(observable_disposable);
            obs.on_error(std::make_exception_ptr(std::runtime_error{""}));
        });

        op(observable) | rpp::ops::subscribe([](const auto&) { }, [&callback_called](const std::exception_ptr&) { CHECK(!callback_called); });
        CHECK(callback_called);
    }

    SUBCASE("operator calls on_completed before dispose")
    {
        bool callback_called       = false;
        auto observable_disposable = rpp::make_callback_disposable([&callback_called]() noexcept {
            callback_called = true;
        });
        auto observable            = rpp::source::create<T>([&observable_disposable](auto&& obs) {
            obs.set_upstream(observable_disposable);
            obs.on_completed();
        });

        op(observable) | rpp::ops::subscribe([](const auto&) { }, [&callback_called]() { CHECK(!callback_called); });
        CHECK(callback_called);
    }
}

template<typename T>
void test_operator_over_observable_with_disposable(auto&& op)
{
    SUBCASE("operator disposes disposable but not too early")
    {
        auto                                    observable_disposable = rpp::composite_disposable_wrapper::make();
        std::optional<rpp::dynamic_observer<T>> saved_observer{};
        auto                                    observable = rpp::source::create<T>([&observable_disposable, &saved_observer](auto&& obs) {
            obs.set_upstream(observable_disposable);
            saved_observer.emplace(std::forward<decltype(obs)>(obs).as_dynamic());
        });

        auto observer_disposable = rpp::composite_disposable_wrapper::make();
        op(observable) | rpp::ops::subscribe(observer_disposable, [](const auto&) { });

        CHECK(!observable_disposable.is_disposed());
        observer_disposable.dispose();
        CHECK(observable_disposable.is_disposed());
    }

    SUBCASE("operator disposes disposable on_error")
    {
        op(rpp::source::create<T>([](auto&& obs) {
            const auto d = rpp::composite_disposable_wrapper::make();
            obs.set_upstream(d);
            obs.on_error({});
            CHECK(obs.is_disposed());
            CHECK(d.is_disposed());
        })).subscribe([](const auto&) { }, [](const std::exception_ptr&) { });
    }

    SUBCASE("operator disposes disposable on_completed")
    {
        op(rpp::source::create<T>([](auto&& obs) {
            const auto d = rpp::composite_disposable_wrapper::make();
            obs.set_upstream(d);
            obs.on_completed();
            CHECK(obs.is_disposed());
            CHECK(d.is_disposed());
        })).subscribe([](const auto&) { }, [](const std::exception_ptr&) { });
    }

    SUBCASE("set_upstream with fixed_disposables_strategy<1>")
    {
        CHECK_NOTHROW(op(rpp::observable<T, wrapped_observable_strategy_set_upstream<T, rpp::details::observables::fixed_disposables_strategy<1>>>{})
                          .subscribe([](const auto&) { }, rpp::utils::rethrow_error_t{}));
    }

    SUBCASE("set_upstream with dynamic_disposables_strategy")
    {
        CHECK_NOTHROW(op(rpp::observable<T, wrapped_observable_strategy_set_upstream<T, rpp::details::observables::dynamic_disposables_strategy>>{})
                          .subscribe([](const auto&) { }, rpp::utils::rethrow_error_t{}));
    }

    SUBCASE("none_disposables_strategy")
    {
        CHECK_NOTHROW(op(rpp::observable<T, wrapped_observable_strategy_no_set_upstream<T, rpp::details::observables::fixed_disposables_strategy<0>>>{})
                          .subscribe([](const auto&) { }, rpp::utils::rethrow_error_t{}));
    }

    SUBCASE("fixed_disposables_strategy<0>")
    {
        CHECK_NOTHROW(op(rpp::observable<T, wrapped_observable_strategy_no_set_upstream<T, rpp::details::observables::fixed_disposables_strategy<0>>>{})
                          .subscribe([](const auto&) { }, rpp::utils::rethrow_error_t{}));
    }

    SUBCASE("dynamic_disposables_strategy")
    {
        CHECK_NOTHROW(op(rpp::observable<T, wrapped_observable_strategy_no_set_upstream<T, rpp::details::observables::dynamic_disposables_strategy>>{})
                          .subscribe([](const auto&) { }, rpp::utils::rethrow_error_t{}));
    }

    SUBCASE("set_upstream with none_disposables_strategy calls on_error")
    {
        CHECK_NOTHROW(op(rpp::observable<T, wrapped_observable_strategy_set_upstream<T, rpp::details::observables::fixed_disposables_strategy<0>>>{})
                          .subscribe([](const auto&) { }, [](const std::exception_ptr& err) { CHECK_THROWS_AS(std::rethrow_exception(err), rpp::utils::more_disposables_than_expected); }));
    }

    SUBCASE("set_upstream with fixed_disposables_strategy<0> calls on_error")
    {
        CHECK_NOTHROW(op(rpp::observable<T, wrapped_observable_strategy_set_upstream<T, rpp::details::observables::fixed_disposables_strategy<0>>>{})
                          .subscribe([](const auto&) { }, [](const std::exception_ptr& err) { CHECK_THROWS_AS(std::rethrow_exception(err), rpp::utils::more_disposables_than_expected); }));
    }
}

template<typename T>
void test_operator_with_disposable(auto&& op)
{
    test_operator_over_observable_with_disposable<T>([op](auto&& observable) { return observable | op; });
}

template<typename T>
void test_operator_finish_before_dispose(auto&& op)
{
    test_operator_over_observable_finish_before_dispose<T>([op](auto&& observable) { return observable | op; });
}
