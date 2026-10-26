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

#include <rpp/schedulers/details/queue.hpp>
#include <rpp/schedulers/details/utils.hpp>
#include <rpp/schedulers/details/worker.hpp>
#include <rpp/utils/functors.hpp>

namespace rpp::schedulers
{
    /**
     * @brief Schedules execution of schedulables via queueing tasks to the caller thread with priority to time_point and order.
     * @warning Caller thread is thread where `schedule` called.
     *
     * @details When this scheduler passed to some operators, then caller thread is thread where scheduling of some action happens. In most cases it is where `on_next` was called.
     *
     * @par Why do we need it?
     * This scheduler used to prevent recursion calls and making planar linear execution of schedulables. For example:
     * @code{.cpp}
     * auto worker = rpp::schedulers::current_thread::create_worker();
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
     * Would lead to:
     * - "Task 1 starts"
     * - "Task 1 ends"
     * - "Task 2 starts"
     * - "Task 2 ends"
     * - "Task 3"
     * - "Task 4"
     *
     * @par How to use it properly?
     * To have any visible impact you need to use it at least **twice** during same observable. For example, `rpp::source::just` source uses it as default scheduler as well as `rpp::operators::merge` operator (which just "owns" it during subscription).
     *
     * For example, this one
     * @code{.cpp}
     * rpp::source::just(1, 2, 3)
     *  | rpp::operators::merge_with(rpp::source::just(4, 5, 6))
     *  | rpp::operators::subscribe([](int v) { std::cout << v << " "; });
     * @endcode
     * Procedes output `1 4 2 5 3 6` due to `merge_with` takes ownership over this scheduler during subscription, both sources schedule their first emissions into scheduler, then `merge_with` frees scheduler and it starts to proceed scheduled actions. As a result it continues interleaving of values. In case of usingg `rpp::schedulers::immediate` it would be:
     * @code{.cpp}
     * rpp::source::just(rpp::schedulers::immediate{}, 1, 2, 3)
     *  | rpp::operators::merge_with(rpp::source::just(rpp::schedulers::immediate{}, 4, 5, 6))
     *  | rpp::operators::subscribe([](int v) { std::cout << v << " "; });
     * @endcode
     * With output `1 2 3 4 5 6`
     *
     * @ingroup schedulers
     */
    class current_thread
    {
    public:
        friend class new_thread;
        class worker_strategy;

        static details::schedulables_queue<worker_strategy>*& get_queue()
        {
            thread_local details::schedulables_queue<worker_strategy>* s_queue{};
            return s_queue;
        }

        struct is_queue_is_empty
        {
            const details::schedulables_queue<worker_strategy>& queue;

            bool operator()() const { return queue.is_empty(); }
        };


        static void drain_queue() noexcept
        {
            while (get_queue() && !get_queue()->is_empty())
            {
                auto top = get_queue()->pop();
                if (top->is_disposed())
                    continue;

                details::sleep_until(top->get_timepoint());

                while (true)
                {
                    if (const auto res = top->make_advanced_call())
                    {
                        if (!top->is_disposed())
                        {
                            if (get_queue()->is_empty())
                            {
                                if (const auto d = std::get_if<delay_from_now>(&res->get()))
                                {
                                    std::this_thread::sleep_for(d->value);
                                }
                                else
                                {
                                    details::sleep_until(top->handle_advanced_call(res.value()));
                                }
                                continue;
                            }
                            const auto tp = top->handle_advanced_call(res.value());
                            get_queue()->emplace(tp, std::move(top));
                        }
                    }
                    break;
                }
            }

            get_queue() = nullptr;
        }

        class worker_strategy
        {
        public:
            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, constraint::schedulable_fn<Handler, Args...> Fn>
            static void defer_for(duration duration, Fn&& fn, Handler&& handler, Args&&... args)
            {
                if (handler.is_disposed())
                    return;

                if (!get_queue())
                {
                    details::schedulables_queue<worker_strategy> queue{};
                    get_queue() = &queue;

                    const auto timepoint = details::immediate_scheduling_while_condition<worker_strategy>(duration, is_queue_is_empty{queue}, fn, handler, args...);
                    if (!timepoint || handler.is_disposed())
                        return drain_queue();

                    get_queue()->emplace(timepoint.value(), std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
                    return drain_queue();
                }

                get_queue()->emplace(now() + duration, std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
            }

            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, constraint::schedulable_fn<Handler, Args...> Fn>
            static void defer_to(time_point tp, Fn&& fn, Handler&& handler, Args&&... args)
            {
                if (handler.is_disposed())
                    return;

                if (get_queue())
                {
                    get_queue()->emplace(tp, std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
                }
                else
                {
                    defer_for(tp - now(), std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
                }
            }

            static rpp::schedulers::time_point now() { return details::now(); }
        };

    private:
        class own_queue_guard
        {
        public:
            own_queue_guard()
                : m_clear_on_destruction{!get_queue()}
            {
                if (m_clear_on_destruction)
                    get_queue() = &m_queue;
            }
            ~own_queue_guard()
            {
                if (m_clear_on_destruction)
                    drain_queue();
            }
            own_queue_guard(const own_queue_guard&) = delete;
            own_queue_guard(own_queue_guard&&)      = delete;

        private:
            details::schedulables_queue<worker_strategy> m_queue{};
            bool                                         m_clear_on_destruction{};
        };

    public:
        static own_queue_guard own_queue_and_drain_finally_if_not_owned()
        {
            return own_queue_guard{};
        }

        static rpp::schedulers::worker<worker_strategy> create_worker()
        {
            return rpp::schedulers::worker<worker_strategy>{};
        }
    };
} // namespace rpp::schedulers
