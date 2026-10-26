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
#include <rpp/utils/constraints.hpp>

#include <unordered_set>

namespace rpp::operators::details
{
    template<rpp::constraint::decayed_type Type, rpp::constraint::observer TObserver>
    struct distinct_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        RPP_NO_UNIQUE_ADDRESS TObserver  observer;
        mutable std::unordered_set<Type> past_values{};

        template<typename T>
        void on_next(T&& v) const
        {
            const auto [it, inserted] = past_values.insert(std::forward<T>(v));
            if (inserted)
            {
                observer.on_next(*it);
            }
        }

        void on_error(const std::exception_ptr& err) const { observer.on_error(err); }

        void on_completed() const { observer.on_completed(); }

        void set_upstream(const disposable_wrapper& d) { observer.set_upstream(d); }

        bool is_disposed() const { return observer.is_disposed(); }
    };

    struct distinct_t : lift_operator<distinct_t>
    {
        using lift_operator<distinct_t>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            static_assert(rpp::constraint::hashable<T>, "T is not hashable");

            using result_type = T;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = distinct_observer_strategy<T, TObserver>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = Prev;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief For each item from this observable, filter out repeated values and emit only items that have not already been emitted
     *
     * @marble distinct
     {
         source observable       : +--1-1-2-2-3-2-1-|
         operator "distinct"     : +--1---2---3-----|
     }
     *
     * @warning This operator keeps an `std::unordered_set<T>` of past values, so std::hash<T> specialization is required.
     *
     * @ingroup filtering_operators
     * @see https://reactivex.io/documentation/operators/distinct.html
     */
    inline auto distinct()
    {
        return details::distinct_t{};
    }
} // namespace rpp::operators
