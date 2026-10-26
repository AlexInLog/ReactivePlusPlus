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
#include <rpp/schedulers/immediate.hpp>

#include <optional>
#include <type_traits>

namespace rpp::operators::details
{
    template<rpp::constraint::observer TObserver, rpp::schedulers::constraint::scheduler Scheduler>
    struct throttle_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        RPP_NO_UNIQUE_ADDRESS TObserver                    observer;
        rpp::schedulers::duration                          duration{};
        mutable std::optional<rpp::schedulers::time_point> last_emission_time_point{};

        template<typename T>
        void on_next(T&& v) const
        {
            const auto now = rpp::schedulers::utils::get_worker_t<Scheduler>::now();
            if (!last_emission_time_point || now >= last_emission_time_point.value() + duration)
            {
                observer.on_next(std::forward<T>(v));
                last_emission_time_point = now;
            }
        }

        void on_error(const std::exception_ptr& err) const { observer.on_error(err); }

        void on_completed() const { observer.on_completed(); }

        void set_upstream(const disposable_wrapper& d) { observer.set_upstream(d); }

        bool is_disposed() const { return observer.is_disposed(); }
    };

    template<rpp::schedulers::constraint::scheduler Scheduler>
    struct throttle_t : lift_operator<throttle_t<Scheduler>, rpp::schedulers::duration>
    {
        using lift_operator<throttle_t<Scheduler>, rpp::schedulers::duration>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            using result_type = T;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = throttle_observer_strategy<TObserver, Scheduler>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = Prev;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
    * @brief Emit emission from an Observable and then ignore subsequent values during `duration` of time.
    *
    * @marble throttle
    {
        source    observable   : +--1-2-----3---|
        operator "throttle(4)" : +--1-------3---|
    }
    *
    * @details Actually this operator just keeps time of last emission, skips values ​​until duration has passed, then forwards value, updates timepoint and etc...
    *
    * @par Performance notes:
    * - No any heap allocations at all
    * - No any copies/moves of emissions, just passing by const& to predicate and then forwarding
    * - Obtaining "now" every emission
    *
    * @param period is period of time to skip subsequent emissions
    * @tparam Scheduler is type used to determine `now()`. Shouldn't be used in production code
    *
    * @note `#include <rpp/operators/throttle.hpp>`
    *
    * @par Example:
    * @snippet throttle.cpp throttle
    *
    * @ingroup filtering_operators
    * @see https://reactivex.io/documentation/operators/debounce.html
    */
    template<rpp::schedulers::constraint::scheduler Scheduler /* = rpp::schedulers::immediate*/>
    auto throttle(rpp::schedulers::duration period)
    {
        return details::throttle_t<std::decay_t<Scheduler>>{period};
    }
} // namespace rpp::operators
