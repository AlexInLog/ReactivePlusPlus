//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/sources/fwd.hpp>

#include <rpp/defs.hpp>
#include <rpp/observables/observable.hpp>

namespace rpp::details
{
    struct interval_schedulable
    {
        rpp::schedulers::optional_delay_from_this_timepoint operator()(const auto& observer, rpp::schedulers::duration period, size_t& counter) const
        {
            observer.on_next(counter++);
            return rpp::schedulers::optional_delay_from_this_timepoint{period};
        }
    };

    template<typename TScheduler, typename TimePointOrDuration>
    struct interval_strategy
    {
        using value_type                   = size_t;
        using optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<0>;

        RPP_NO_UNIQUE_ADDRESS TScheduler scheduler;
        TimePointOrDuration              initial;
        rpp::schedulers::duration        period;

        template<rpp::constraint::observer_of_type<value_type> TObs>
        void subscribe(TObs&& observer) const
        {
            const auto worker = scheduler.create_worker();
            worker.schedule(initial, interval_schedulable{}, std::forward<TObs>(observer), period, size_t{});
        }
    };
} // namespace rpp::details

namespace rpp
{
    template<schedulers::constraint::scheduler TScheduler, typename TimePointOrDuration>
    using interval_observable = observable<size_t, details::interval_strategy<TScheduler, TimePointOrDuration>>;
} // namespace rpp

namespace rpp::source
{

    /**
     * @brief Creates rpp::observable that emits a sequential integer every specified time interval, on the specified scheduler.
     *
     * @marble interval_with_initial
       {
           operator "interval(20s, 10s)": +--1-2-3-5->
       }
     *
     * @param initial duration before first emission
     * @param period period between emitted values
     * @param scheduler the scheduler to use for scheduling the items
     *
     * @par Example:
     * @snippet interval.cpp interval period
     *
     * @ingroup creational_operators
     * @see https://reactivex.io/documentation/operators/interval.html
     */
    template<schedulers::constraint::scheduler TScheduler>
    auto interval(rpp::schedulers::duration initial, rpp::schedulers::duration period, TScheduler&& scheduler)
    {
        return interval_observable<std::decay_t<TScheduler>, rpp::schedulers::duration>{std::forward<TScheduler>(scheduler), initial, period};
    }

    /**
     * @brief Same rpp::source::interval but using a time_point as initial time instead of a duration.
     *
     * @param initial time_point before first emission
     * @param period period between emitted values
     * @param scheduler the scheduler to use for scheduling the items
     *
     * @ingroup creational_operators
     * @see https://reactivex.io/documentation/operators/interval.html
     */
    template<schedulers::constraint::scheduler TScheduler>
    auto interval(rpp::schedulers::time_point initial, rpp::schedulers::duration period, TScheduler&& scheduler)
    {
        return interval_observable<std::decay_t<TScheduler>, rpp::schedulers::time_point>{std::forward<TScheduler>(scheduler), initial, period};
    }

    /**
     * @brief Creates rpp::observable that emits a sequential integer every specified time interval, on the specified scheduler.
     *
     * @marble interval
       {
           operator "interval(10s)": +-1-2-3-5->
       }
     * @param period period between emitted values
     * @param scheduler the scheduler to use for scheduling the items
     *
     * @par Example:
     * @snippet interval.cpp interval period
     *
     * @ingroup creational_operators
     * @see https://reactivex.io/documentation/operators/interval.html
     */
    template<schedulers::constraint::scheduler TScheduler>
    auto interval(rpp::schedulers::duration period, TScheduler&& scheduler)
    {
        return interval(period, period, std::forward<TScheduler>(scheduler));
    }
} // namespace rpp::source
