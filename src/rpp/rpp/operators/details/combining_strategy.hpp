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

#include <rpp/operators/fwd.hpp>

#include <rpp/defs.hpp>
#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/operators/details/strategy.hpp>
#include <rpp/schedulers/current_thread.hpp>
#include <rpp/utils/utils.hpp>

#include <memory>

namespace rpp::operators::details
{
    template<rpp::constraint::observer Observer>
    class combining_disposable : public composite_disposable
    {
    public:
        explicit combining_disposable(Observer&& observer, size_t on_completed_needed)
            : m_observer_with_mutex{std::move(observer)}
            , m_on_completed_needed{on_completed_needed}
        {
        }

        rpp::utils::pointer_under_lock<Observer> get_observer_under_lock() { return m_observer_with_mutex; }

        bool decrement_on_completed()
        {
            // just need atomicity, not guarding anything
            return m_on_completed_needed.fetch_sub(1, std::memory_order::seq_cst) == 1;
        }

    private:
        rpp::utils::value_with_mutex<Observer> m_observer_with_mutex{};

        std::atomic_size_t m_on_completed_needed;
    };

    template<typename TDisposable>
    struct combining_observer_strategy
    {
        // `Auto` due to we have to dispose disposables during on_completed anyway
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::Auto;

        std::shared_ptr<TDisposable> disposable{};

        void set_upstream(const rpp::disposable_wrapper& d) const
        {
            disposable->add(d);
        }

        bool is_disposed() const
        {
            return disposable->is_disposed();
        }

        void on_error(const std::exception_ptr& err) const
        {
            disposable->get_observer_under_lock()->on_error(err);
        }

        void on_completed() const
        {
            if (disposable->decrement_on_completed())
                disposable->get_observer_under_lock()->on_completed();
        }
    };

    template<template<typename...> typename TDisposable, template<auto, typename...> typename TStrategy, typename TSelector, rpp::constraint::observable... TObservables>
    struct combining_operator_t
    {
        RPP_NO_UNIQUE_ADDRESS rpp::utils::tuple<TObservables...> observables;
        RPP_NO_UNIQUE_ADDRESS TSelector                          selector;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            static_assert(std::invocable<TSelector, T, rpp::utils::extract_observable_type_t<TObservables>...>, "Selector is not callable with passed T type");

            using result_type = std::invoke_result_t<TSelector, T, rpp::utils::extract_observable_type_t<TObservables>...>;

            constexpr static bool own_current_queue = true;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = ::rpp::details::observables::fixed_disposables_strategy<1>;

        template<rpp::constraint::decayed_type Type, rpp::constraint::observer Observer>
        auto lift(Observer&& observer) const
        {
            return observables.apply(&subscribe_impl<Type, Observer>, std::forward<Observer>(observer), selector);
        }

    private:
        template<rpp::constraint::decayed_type Type, rpp::constraint::observer Observer>
        static auto subscribe_impl(Observer&& observer, const TSelector& selector, const TObservables&... observables)
        {
            using Disposable = TDisposable<Observer, TSelector, Type, rpp::utils::extract_observable_type_t<TObservables>...>;

            const auto disposable = disposable_wrapper_impl<Disposable>::make(std::forward<Observer>(observer), selector);
            auto       locked     = disposable.lock();
            locked->get_observer_under_lock()->set_upstream(disposable.as_weak());

            subscribe<std::decay_t<Type>>(locked, std::index_sequence_for<TObservables...>{}, observables...);

            return rpp::observer<Type, TStrategy<0, std::decay_t<Observer>, TSelector, Type, rpp::utils::extract_observable_type_t<TObservables>...>>{std::move(locked)};
        }

        template<typename ExpectedValue, rpp::constraint::observer Observer, size_t... I>
        static void subscribe(const std::shared_ptr<TDisposable<Observer, TSelector, ExpectedValue, rpp::utils::extract_observable_type_t<TObservables>...>>& disposable, std::index_sequence<I...>, const TObservables&... observables)
        {
            (..., observables.subscribe(rpp::observer<rpp::utils::extract_observable_type_t<TObservables>, TStrategy<I + 1, Observer, TSelector, ExpectedValue, rpp::utils::extract_observable_type_t<TObservables>...>>{disposable}));
        }
    };
} // namespace rpp::operators::details
