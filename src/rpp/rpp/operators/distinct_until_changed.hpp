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
#include <rpp/operators/details/strategy.hpp>

#include <type_traits>

namespace rpp::operators::details
{
    template<rpp::constraint::decayed_type Type, rpp::constraint::observer TObserver, rpp::constraint::decayed_type EqualityFn>
    struct distinct_until_changed_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        RPP_NO_UNIQUE_ADDRESS TObserver  observer;
        RPP_NO_UNIQUE_ADDRESS EqualityFn comparator;
        mutable std::optional<Type>      last_value{};

        template<typename T>
        void on_next(T&& v) const
        {
            if (last_value.has_value() && comparator(utils::as_const(last_value.value()), rpp::utils::as_const(v)))
                return;

            last_value.emplace(std::forward<T>(v));
            observer.on_next(utils::as_const(last_value.value()));
        }

        void on_error(const std::exception_ptr& err) const { observer.on_error(err); }

        void on_completed() const { observer.on_completed(); }

        void set_upstream(const disposable_wrapper& d) { observer.set_upstream(d); }

        bool is_disposed() const { return observer.is_disposed(); }
    };

    template<rpp::constraint::decayed_type EqualityFn>
    struct distinct_until_changed_t : public operators::details::lift_operator<distinct_until_changed_t<EqualityFn>, EqualityFn>
    {
        using operators::details::lift_operator<distinct_until_changed_t<EqualityFn>, EqualityFn>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            static_assert(rpp::constraint::invocable_r_v<bool, EqualityFn, T, T>, "EqualityFn is not invocable with T and T returning bool");

            using result_type = T;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = distinct_until_changed_observer_strategy<T, TObserver, EqualityFn>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = Prev;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Suppress consecutive duplicates of emissions from original observable
     *
     * @marble distinct_until_changed
     {
         source observable       : +--1-1-2-2-3-2-1-|
         operator "distinct_until_changed" : +--1---2---3-2-1-|
     }
     *
     * @details Actually this operator has `std::optional` with last item and checks everytime where new emission is same or not.
     *
     * @par Performance notes:
     * - No any heap allocations at all
     * - std::optional to keep last value
     * - passing last and emitted value to predicate
     *
     * @param equality_fn optional equality comparator function
     * @note `#include <rpp/operators/distinct_until_changed.hpp>`
     *
     * @par Example
     * @snippet distinct_until_changed.cpp distinct_until_changed
     * @snippet distinct_until_changed.cpp distinct_until_changed_with_comparator
     *
     * @ingroup filtering_operators
     * @see https://reactivex.io/documentation/operators/distinct.html
     */
    template<typename EqualityFn>
        requires (!utils::is_not_template_callable<EqualityFn> || std::same_as<bool, std::invoke_result_t<EqualityFn, rpp::utils::convertible_to_any, rpp::utils::convertible_to_any>>)
    auto distinct_until_changed(EqualityFn&& equality_fn)
    {
        return details::distinct_until_changed_t<std::decay_t<EqualityFn>>{std::forward<EqualityFn>(equality_fn)};
    }
} // namespace rpp::operators
