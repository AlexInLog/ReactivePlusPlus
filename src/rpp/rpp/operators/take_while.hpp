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
    template<rpp::constraint::observer TObserver, rpp::constraint::decayed_type Fn>
    struct take_while_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        RPP_NO_UNIQUE_ADDRESS TObserver observer;
        RPP_NO_UNIQUE_ADDRESS Fn        fn;

        template<typename T>
        void on_next(T&& v) const
        {
            if (fn(rpp::utils::as_const(v)))
                observer.on_next(std::forward<T>(v));
            else
                observer.on_completed();
        }

        void on_error(const std::exception_ptr& err) const { observer.on_error(err); }

        void on_completed() const { observer.on_completed(); }

        void set_upstream(const disposable_wrapper& d) { observer.set_upstream(d); }

        bool is_disposed() const { return observer.is_disposed(); }
    };

    template<rpp::constraint::decayed_type Fn>
    struct take_while_t : lift_operator<take_while_t<Fn>, Fn>
    {
        using lift_operator<take_while_t<Fn>, Fn>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            static_assert(std::is_invocable_r_v<bool, Fn, T>, "Fn is not invocable with T returning bool");

            using result_type = T;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = take_while_observer_strategy<TObserver, Fn>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = Prev;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Sends items from observable while items are satisfy predicate. When condition becomes false -> sends `on_completed`
     *
     * @marble take_while
     {
         source observable                : +--1-2-3-4-5-6-|
         operator "take_while: x => x!=3" : +--1-2-|
     }
     *
     * @details Actually this operator just emits values while predicate returns true
     *
     * @par Performance notes:
     * - No any heap allocations at all
     * - No any copies/moves of emissions, just passing to predicate by const& and then forwarding
     *
     * @param predicate is predicate used to check items. Accepts value from observable and returns `true` if value should be forwarded and `false` if emissions should be stopped and observable should be terminated.
     * @note `#include <rpp/operators/take_while.hpp>`
     *
     * @par Example:
     * @snippet take_while.cpp take_while
     *
     * @ingroup conditional_operators
     * @see https://reactivex.io/documentation/operators/takewhile.html
     */
    template<typename Fn>
        requires (!utils::is_not_template_callable<Fn> || std::same_as<bool, std::invoke_result_t<Fn, rpp::utils::convertible_to_any>>)
    auto take_while(Fn&& predicate)
    {
        return details::take_while_t<std::decay_t<Fn>>{std::forward<Fn>(predicate)};
    }
} // namespace rpp::operators
