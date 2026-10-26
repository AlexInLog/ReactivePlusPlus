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

namespace rpp::operators::details
{
    template<rpp::constraint::observer TObserver>
    struct first_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        RPP_NO_UNIQUE_ADDRESS TObserver observer;

        template<typename T>
        void on_next(T&& v) const
        {
            observer.on_next(std::forward<T>(v));
            observer.on_completed();
        }

        void on_completed() const
        {
            observer.on_error(std::make_exception_ptr(utils::not_enough_emissions{"first() operator expects at least one emission from observable before completion"}));
        }

        void on_error(const std::exception_ptr& err) const { observer.on_error(err); }

        void set_upstream(const disposable_wrapper& d) { observer.set_upstream(d); }

        bool is_disposed() const { return observer.is_disposed(); }
    };

    struct first_t : lift_operator<first_t>
    {
        using lift_operator<first_t>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            using result_type = T;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = first_observer_strategy<TObserver>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = Prev;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Emit only the first item.
     *
     * @marble first
         {
             source observable   : +--1--2--3--|
             operator "first"    : +--1|
         }
     *
     * @details Actually this operator is `take(1)` with exception during `on_completed` if no any emision happens. So, it just forwards first obtained emission and emits on_completed immediately
     * @throws rpp::utils::not_enough_emissions in case of on_completed obtained without any emissions
     *
     * @par Performance notes:
     * - No any heap allocations
     * - No any copies/moves just forwarding of emission
     *
     * @note `#include <rpp/operators/first.hpp>`
     *
     * @par Example:
     * @snippet first.cpp first
     * @snippet first.cpp first_empty
     *
     * @ingroup filtering_operators
     * @see https://reactivex.io/documentation/operators/first.html
     */
    inline auto first()
    {
        return details::first_t{};
    }
} // namespace rpp::operators
