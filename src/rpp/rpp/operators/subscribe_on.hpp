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
#include <rpp/observables/observable.hpp>

namespace rpp::operators::details
{
    template<typename TObservableChainStrategy>
    struct subscribe_on_schedulable
    {
        RPP_NO_UNIQUE_ADDRESS TObservableChainStrategy observable;

        using Type = typename TObservableChainStrategy::value_type;

        template<rpp::constraint::observer_strategy<Type> ObserverStrategy>
        rpp::schedulers::optional_delay_from_now operator()(observer<Type, ObserverStrategy>& observer) const
        {
            observable.subscribe(std::move(observer));
            return rpp::schedulers::optional_delay_from_now{};
        }
    };

    template<rpp::schedulers::constraint::scheduler TScheduler>
    struct subscribe_on_t
    {
        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            using result_type = T;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = Prev;

        RPP_NO_UNIQUE_ADDRESS TScheduler scheduler;

        template<rpp::constraint::observer Observer, typename... Strategies>
        void subscribe(Observer&& observer, const rpp::details::observables::chain<Strategies...>& observable_strategy) const
        {
            const auto worker = scheduler.create_worker();
            worker.schedule(subscribe_on_schedulable<rpp::details::observables::chain<Strategies...>>{observable_strategy}, std::forward<Observer>(observer));
        }
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief OnSubscribe function for this observable will be scheduled via provided scheduler
     *
     * @details Actually this operator just schedules subscription on original observable to provided scheduler
     *
     * @param scheduler is scheduler used for scheduling of OnSubscribe
     * @note `#include <rpp/operators/subscribe_on.hpp>`
     *
     * @par Example:
     * @snippet subscribe_on.cpp subscribe_on
     *
     * @ingroup utility_operators
     * @see https://reactivex.io/documentation/operators/subscribeon.html
     */
    template<rpp::schedulers::constraint::scheduler Scheduler>
    auto subscribe_on(Scheduler&& scheduler)
    {
        return details::subscribe_on_t<std::decay_t<Scheduler>>{std::forward<Scheduler>(scheduler)};
    }
} // namespace rpp::operators
