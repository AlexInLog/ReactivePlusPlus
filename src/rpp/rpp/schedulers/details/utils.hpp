//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2022 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/schedulers/fwd.hpp>

#include <exception>
#include <optional>
#include <thread>

namespace rpp::schedulers::details
{
    inline thread_local time_point s_last_now_time{};

    inline rpp::schedulers::time_point now()
    {
        return s_last_now_time = clock_type::now();
    }

    inline bool sleep_until(const time_point timepoint)
    {
        if (timepoint <= details::s_last_now_time)
            return false;

        const auto now = clock_type::now();
        std::this_thread::sleep_for(timepoint - now);
        details::s_last_now_time = std::max(now, timepoint);
        return timepoint > now;
    }

    /**
     * @brief Makes immediate-like scheduling for provided arguments
     * @returns nullopt in case of subscription unsubscribed or schedulable doesn't requested to re-schedule, some value - in case of condition failed but still some duration to delay action
     **/
    template<typename NowStrategy, rpp::schedulers::constraint::schedulable_handler Handler, typename... Args>
    std::optional<time_point> immediate_scheduling_while_condition(duration                                                                      duration,
                                                                   const std::predicate auto&                                                    condition,
                                                                   constraint::schedulable_delay_from_this_timepoint_fn<Handler, Args...> auto&& fn,
                                                                   Handler&&                                                                     handler,
                                                                   Args&&... args) noexcept
    {
        auto timepoint = NowStrategy::now() + duration;
        while (condition())
        {
            if (handler.is_disposed())
                return std::nullopt;

            if (sleep_until(timepoint) && handler.is_disposed())
                return std::nullopt;

            try
            {
                if (const auto duration_from_timepoint = fn(handler, args...))
                    timepoint += duration_from_timepoint->value;
                else
                    return std::nullopt;
            }
            catch (...)
            {
                handler.on_error(std::current_exception());
                return std::nullopt;
            }
        }

        return timepoint;
    }

    /**
     * @brief Makes immediate-like scheduling for provided arguments
     * @returns nullopt in case of subscription unsubscribed or schedulable doesn't requested to re-schedule, some value - in case of condition failed but still some duration to delay action
     **/
    template<typename NowStrategy, rpp::schedulers::constraint::schedulable_handler Handler, typename... Args>
    std::optional<time_point> immediate_scheduling_while_condition(duration                                                           duration,
                                                                   const std::predicate auto&                                         condition,
                                                                   constraint::schedulable_delay_from_now_fn<Handler, Args...> auto&& fn,
                                                                   Handler&&                                                          handler,
                                                                   Args&&... args) noexcept
    {
        while (condition())
        {
            if (handler.is_disposed())
                return std::nullopt;

            if (duration > duration::zero())
            {
                std::this_thread::sleep_for(duration);

                if (handler.is_disposed())
                    return std::nullopt;
            }

            try
            {
                if (const auto new_duration = fn(handler, args...))
                    duration = new_duration->value;
                else
                    return std::nullopt;
            }
            catch (...)
            {
                handler.on_error(std::current_exception());
                return std::nullopt;
            }
        }

        return NowStrategy::now() + duration;
    }

    /**
     * @brief Makes immediate-like scheduling for provided arguments
     * @returns nullopt in case of subscription unsubscribed or schedulable doesn't requested to re-schedule, some value - in case of condition failed but still some duration to delay action
     **/
    template<typename NowStrategy, rpp::schedulers::constraint::schedulable_handler Handler, typename... Args>
    std::optional<time_point> immediate_scheduling_while_condition(duration                                                     duration,
                                                                   const std::predicate auto&                                   condition,
                                                                   constraint::schedulable_delay_to_fn<Handler, Args...> auto&& fn,
                                                                   Handler&&                                                    handler,
                                                                   Args&&... args) noexcept
    {
        std::optional<time_point> timepoint{};
        while (condition())
        {
            if (handler.is_disposed())
                return std::nullopt;

            if (!timepoint.has_value())
            {
                if (duration > duration::zero())
                {
                    std::this_thread::sleep_for(duration);

                    if (handler.is_disposed())
                        return std::nullopt;
                }
            }
            else if (sleep_until(timepoint.value()) && handler.is_disposed())
                return std::nullopt;

            try
            {
                if (const auto new_timepoint = fn(handler, args...))
                    timepoint = new_timepoint->value;
                else
                    return std::nullopt;
            }
            catch (...)
            {
                handler.on_error(std::current_exception());
                return std::nullopt;
            }
        }

        return timepoint;
    }
} // namespace rpp::schedulers::details
