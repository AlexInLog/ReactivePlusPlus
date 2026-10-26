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
    template<rpp::constraint::observer TObserver, rpp::constraint::decayed_type Fn>
    struct map_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        RPP_NO_UNIQUE_ADDRESS TObserver observer;
        RPP_NO_UNIQUE_ADDRESS Fn        fn;

        template<typename T>
        void on_next(T&& v) const
        {
            observer.on_next(fn(std::forward<T>(v)));
        }

        void on_error(const std::exception_ptr& err) const { observer.on_error(err); }

        void on_completed() const { observer.on_completed(); }

        void set_upstream(const disposable_wrapper& d) { observer.set_upstream(d); }

        bool is_disposed() const { return observer.is_disposed(); }
    };

    template<rpp::constraint::decayed_type Fn>
    struct map_t : lift_operator<map_t<Fn>, Fn>
    {
        using lift_operator<map_t<Fn>, Fn>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            static_assert(!std::same_as<void, std::invoke_result_t<Fn, T>>, "Fn is not invocable with T");

            using result_type = std::invoke_result_t<Fn, T>;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = map_observer_strategy<TObserver, Fn>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = Prev;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Transforms the items emitted by an Observable via applying a function to each item and emitting result
     * @note The Map operator can keep same type of value or change it to some another type.
     *
     * @marble map
     {
         source observable       : +--1   -2   --3   -|
         operator "map: x=>x+10" : +--(11)-(12)--(13)-|
     }
     *
     * @details Actually this operator just applies callable to each obtained emission and emit resulting value
     *
     * @par Performance notes:
     * - No any heap allocations at all
     * - No any copies/moves of emissions, just forwarding to callable
     *
     * @param callable is callable used to provide this transformation. Should accept `Type` of original observable and return type for new observable
     * @note `#include <rpp/operators/map.hpp>`
     *
     * @par Example with same type:
     * @snippet map.cpp Same type
     *
     * @par Example with changed type:
     * @snippet map.cpp Changed type
     *
     * @ingroup transforming_operators
     * @see https://reactivex.io/documentation/operators/map.html
     */
    template<typename Fn>
        requires (!utils::is_not_template_callable<Fn> || !std::same_as<void, std::invoke_result_t<Fn, rpp::utils::convertible_to_any>>)
    auto map(Fn&& callable)
    {
        return details::map_t<std::decay_t<Fn>>{std::forward<Fn>(callable)};
    }
} // namespace rpp::operators
