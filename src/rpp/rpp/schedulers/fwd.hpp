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

#include <rpp/disposables/fwd.hpp>

#include <rpp/utils/constraints.hpp>

#include <chrono>
#include <optional>

namespace rpp::schedulers
{
    using clock_type = std::chrono::steady_clock;
    using time_point = clock_type::time_point;
    using duration   = std::chrono::nanoseconds;

    /**
     * @brief Timepoint of next execution would be calculcated from NOW timpoint (time of returning from schedulable)
     *
     * @details Implementation looks like this
     * @code{.cpp}
     * const auto duration_from_now = schedulable();
     * schedule(now() + duration_from_now, schedulable);
     * @endcode
     */
    struct delay_from_now
    {
        explicit delay_from_now(duration duration = {})
            : value{duration}
        {
        }

        duration value;
    };

    /**
     * @brief Timepoint of next execution would be calculcated from timepoint of current scheduling
     *
     * @details Implementation looks like this
     * @code{.cpp}
     * const auto timepoint_for_schedulable = schedulable->get_timepoint();
     * sleep_until(timepoint_for_schedulable);
     * const auto duration_from_this_timepoint = schedulable();
     * schedule(timepoint_for_schedulable + duration_from_this_timepoint, schedulable);
     * @endcode
     */
    struct delay_from_this_timepoint
    {
        explicit delay_from_this_timepoint(duration duration = {})
            : value{duration}
        {
        }

        duration value;
    };

    /**
     * @brief Provide timepoint of next execution explicitly
     */
    struct delay_to
    {
        explicit delay_to(time_point timepoint = {})
            : value{timepoint}
        {
        }

        time_point value;
    };

    using optional_delay_from_now            = std::optional<delay_from_now>;
    using optional_delay_from_this_timepoint = std::optional<delay_from_this_timepoint>;
    using optional_delay_to                  = std::optional<delay_to>;
} // namespace rpp::schedulers

namespace rpp::schedulers::details
{
    struct fake_schedulable_handler
    {
        constexpr static bool is_disposed() { return true; }

        static void on_error(const std::exception_ptr&) { }
    };
} // namespace rpp::schedulers::details

namespace rpp::schedulers::constraint
{
    // returns std::nullopt in case of don't need to re-schedule schedulable or some duration which will be added to "now" and re-scheduled
    template<typename Fn, typename... Args>
    concept schedulable_delay_from_now_fn = std::is_invocable_r_v<optional_delay_from_now, Fn, Args&...> && std::same_as<std::invoke_result_t<Fn, Args&...>, optional_delay_from_now>;

    // returns std::nullopt in case of don't need to re-schedule schedulable or some duration which will be added to "now" and re-scheduled
    template<typename Fn, typename... Args>
    concept schedulable_delay_from_this_timepoint_fn = std::is_invocable_r_v<optional_delay_from_this_timepoint, Fn, Args&...> && std::same_as<std::invoke_result_t<Fn, Args&...>, optional_delay_from_this_timepoint>;

    // returns std::nullopt in case of don't need to re-schedule schedulable or some duration which will be added to "now" and re-scheduled
    template<typename Fn, typename... Args>
    concept schedulable_delay_to_fn = std::is_invocable_r_v<optional_delay_to, Fn, Args&...> && std::same_as<std::invoke_result_t<Fn, Args&...>, optional_delay_to>;

    // returns std::nullopt in case of don't need to re-schedule schedulable or one of `delay_from_now` or `delay_from_this_timepoint`
    template<typename Fn, typename... Args>
    concept schedulable_fn = schedulable_delay_from_now_fn<Fn, Args...> || schedulable_delay_from_this_timepoint_fn<Fn, Args...> || schedulable_delay_to_fn<Fn, Args...>;

    template<typename Handler>
    concept schedulable_handler = requires(const Handler& handler) {
        {
            handler.is_disposed()
        } -> std::same_as<bool>;
        handler.on_error(std::exception_ptr{});
    };

    template<typename S>
    concept defer_for_strategy = requires(const S& s, const details::fake_schedulable_handler& handler) {
        {
            s.defer_for(duration{}, std::declval<optional_delay_from_now (*)(const details::fake_schedulable_handler&)>(), handler)
        } -> std::same_as<void>;
        {
            s.defer_for(duration{}, std::declval<optional_delay_from_this_timepoint (*)(const details::fake_schedulable_handler&)>(), handler)
        } -> std::same_as<void>;
        {
            s.defer_for(duration{}, std::declval<optional_delay_to (*)(const details::fake_schedulable_handler&)>(), handler)
        } -> std::same_as<void>;
    };

    template<typename S>
    concept defer_to_strategy = requires(const S& s, const details::fake_schedulable_handler& handler) {
        {
            s.defer_to(time_point{}, std::declval<optional_delay_from_now (*)(const details::fake_schedulable_handler&)>(), handler)
        } -> std::same_as<void>;
        {
            s.defer_to(time_point{}, std::declval<optional_delay_from_this_timepoint (*)(const details::fake_schedulable_handler&)>(), handler)
        } -> std::same_as<void>;
        {
            s.defer_to(time_point{}, std::declval<optional_delay_to (*)(const details::fake_schedulable_handler&)>(), handler)
        } -> std::same_as<void>;
    };

    template<typename S>
    concept strategy = (defer_for_strategy<S> || defer_to_strategy<S>) && requires() {
        {
            S::now()
        } -> std::same_as<rpp::schedulers::time_point>;
    };
} // namespace rpp::schedulers::constraint

namespace rpp::schedulers
{
    template<rpp::schedulers::constraint::strategy Strategy>
    class worker;

    class immediate;
    class current_thread;
    class new_thread;
    class run_loop;
    class thread_pool;
    class computational;

    namespace defaults
    {
        using iteration_scheduler = current_thread;
    } // namespace defaults
} // namespace rpp::schedulers

namespace rpp::schedulers::constraint
{
    namespace details
    {
        template<typename T>
        struct is_worker : std::false_type
        {
        };

        template<constraint::strategy Strategy>
        struct is_worker<rpp::schedulers::worker<Strategy>> : std::true_type
        {
        };
    } // namespace details

    template<typename W>
    concept worker = details::is_worker<W>::value;

    template<typename S>
    concept scheduler = requires(const S& s) {
        {
            s.create_worker()
        } -> worker;
    };
} // namespace rpp::schedulers::constraint

namespace rpp::schedulers::utils
{
    template<rpp::schedulers::constraint::scheduler Scheduler>
    using get_worker_t = std::decay_t<decltype(std::declval<Scheduler>().create_worker())>;
} // namespace rpp::schedulers::utils
