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

#include <rpp/schedulers/fwd.hpp>

#include <rpp/disposables/disposable_wrapper.hpp>
#include <rpp/schedulers/details/utils.hpp>
#include <rpp/schedulers/details/worker.hpp>
#include <rpp/utils/functors.hpp>

namespace rpp::schedulers
{
    /**
     * @brief immediately calls provided schedulable or waits for time_point (in the caller-thread)
     * @par Example
     * @code{.cpp}
     * auto worker = rpp::schedulers::immediate::create_worker();
     * worker.schedule([&worker](const auto& handler)
     * {
     *     std::cout << "Task 1 starts" << std::endl;
     *
     *     worker.schedule([&worker](const auto& handler)
     *     {
     *         std::cout << "Task 2 starts" << std::endl;
     *         worker.schedule([](const auto&)
     *         {
     *             std::cout << "Task 4" << std::endl;
     *             return rpp::schedulers::optional_delay_from_now{};
     *         }, handler);
     *         std::cout << "Task 2 ends" << std::endl;
     *         return rpp::schedulers::optional_delay_from_now{};
     *     }, handler);
     *
     *     worker.schedule([](const auto&)
     *     {
     *         std::cout << "Task 3" << std::endl;
     *         return rpp::schedulers::optional_delay_from_now{};
     *     }, handler);
     *
     *     std::cout << "Task 1 ends" << std::endl;
     *     return rpp::schedulers::optional_delay_from_now{};
     * }, handler);
     * @endcode
     *
     * Would lead to:
     * - "Task 1 starts"
     * - "Task 2 starts"
     * - "Task 4"
     * - "Task 2 ends"
     * - "Task 3"
     * - "Task 1 ends"
     *
     * @ingroup schedulers
     */
    class immediate final
    {
    public:
        class worker_strategy
        {
        public:
            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, constraint::schedulable_fn<Handler, Args...> Fn>
            static void defer_for(duration duration, Fn&& fn, Handler&& handler, Args&&... args)
            {
                details::immediate_scheduling_while_condition<worker_strategy>(duration, rpp::utils::return_true{}, std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
            }

            static rpp::schedulers::time_point now() { return rpp::schedulers::clock_type::now(); }
        };

        static rpp::schedulers::worker<worker_strategy> create_worker()
        {
            return rpp::schedulers::worker<worker_strategy>{};
        }
    };
} // namespace rpp::schedulers
