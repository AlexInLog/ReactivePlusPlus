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

#include <rpp/operators/delay.hpp>

namespace rpp::operators
{
    /**
     * @brief Specify the Scheduler on which an observer will observe this Observable
     * @details The observe_on operator modifies its source Observable by emitting all emissions via provided scheduler, so, all emissions/callbacks happens via scheduler.
     *
     * @marble observe_on
        {
            source observable           : +-1-2-3-#
            operator "observe_on:(--)"  : +---1-2-#
        }
     *
     * @details Actually this operator is just `delay`, but in case of obtaining `on_error` this operator cancels all scheduled but not emited emissions and forward error immediately. In case of you need to delay also `on_error`, use `delay` instead.
     *
     * @param scheduler provides the threading model for delay. e.g. With a new thread scheduler, the observer sees the values in a new thread after a delay duration to the subscription.
     * @param delay_duration is the delay duration for emitting items. Delay duration should be able to cast to rpp::schedulers::duration.
     * @note `#include <rpp/operators/observe_on.hpp>`
     *
     * @par Examples
     * @snippet observe_on.cpp observe_on
     *
     * @ingroup utility_operators
     * @see https://reactivex.io/documentation/operators/observeon.html
     */
    template<rpp::schedulers::constraint::scheduler Scheduler>
    auto observe_on(Scheduler&& scheduler, rpp::schedulers::duration delay_duration)
    {
        return details::delay_t<std::decay_t<Scheduler>, true>{delay_duration, std::forward<Scheduler>(scheduler)};
    }
} // namespace rpp::operators
