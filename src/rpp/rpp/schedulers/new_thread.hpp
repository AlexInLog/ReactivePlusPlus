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

#include <rpp/disposables/details/base_disposable.hpp>
#include <rpp/schedulers/current_thread.hpp>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

namespace rpp::schedulers
{
    /**
     * @brief Scheduler which schedules invoking of schedulables to another thread via queueing tasks with priority to time_point and order
     * @warning Creates new thread for each "create_worker" call, but not for each schedule
     * @details This scheduler useful when we want to have separate thread for processing starting from some timepoint.
     * @ingroup schedulers
     */
    class new_thread
    {
        class state_t final
        {
        public:
            state_t() = default;

            ~state_t() noexcept
            {
                if (!m_thread.joinable())
                    return;

                {
                    std::lock_guard lock{m_state->mutex};
                    m_state->is_stoping = true;
                }
                m_state->cv.notify_all();
                m_thread.detach();
            }

            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, constraint::schedulable_fn<Handler, Args...> Fn>
            void defer_to(time_point time_point, Fn&& fn, Handler&& handler, Args&&... args)
            {
                m_state->queue.emplace(time_point, std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
                m_state->has_fresh_data.store(true);
            }

        private:
            struct queue_data : public details::shared_queue_data
            {
                details::schedulables_queue<current_thread::worker_strategy> queue{};
                bool                                                         is_stoping{};
                std::atomic_bool                                             has_fresh_data{false};
            };

            static void data_thread(std::shared_ptr<queue_data> state)
            {
                current_thread::get_queue() = &state->queue;

                while (true)
                {
                    std::unique_lock lock{state->mutex};
                    if (state->queue.is_empty() && state->is_stoping)
                        break;

                    state->cv.wait(lock, [&] { return !state->queue.is_empty() || state->is_stoping; });

                    if (state->queue.is_empty())
                        break;

                    if (state->queue.top()->is_disposed())
                    {
                        state->queue.pop();
                        continue;
                    }

                    if (details::s_last_now_time < state->queue.top()->get_timepoint())
                    {
                        if (const auto now = worker_strategy::now(); now < state->queue.top()->get_timepoint())
                        {
                            // Sleep until the current top is due, and wake as soon as the top changes
                            // (an earlier schedulable was queued), so the loop recomputes the wait.
                            // The copy keeps the waited-for top alive, so its address cannot be reused
                            // by a newly queued schedulable while we wait.
                            const auto top = state->queue.top();
                            state->cv.wait_until(lock, top->get_timepoint(), [&] { return state->queue.is_empty() || state->queue.top().get() != top.get() || state->queue.top()->is_disposed() || worker_strategy::now() >= state->queue.top()->get_timepoint(); });
                            continue;
                        }
                    }

                    auto top = state->queue.pop();
                    state->has_fresh_data.store(!state->queue.is_empty());
                    lock.unlock();

                    while (true)
                    {
                        if (const auto res = top->make_advanced_call())
                        {
                            if (!top->is_disposed())
                            {
                                if (res->can_run_immediately() && !state->has_fresh_data.load())
                                    continue;

                                const auto tp = top->handle_advanced_call(res.value());
                                state->queue.emplace(tp, std::move(top));
                            }
                        }
                        break;
                    }
                }

                current_thread::get_queue() = nullptr;
            }

        private:
            std::shared_ptr<queue_data> m_state = std::make_shared<queue_data>();

            RPP_CALL_DURING_CONSTRUCTION(m_state->queue = details::schedulables_queue<current_thread::worker_strategy>(m_state));

            std::thread m_thread{&data_thread, m_state};
        };

    public:
        class worker_strategy
        {
        public:
            worker_strategy() = default;

            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, constraint::schedulable_fn<Handler, Args...> Fn>
            void defer_to(time_point tp, Fn&& fn, Handler&& handler, Args&&... args) const
            {
                m_state->defer_to(tp, std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
            }

            static rpp::schedulers::time_point now() { return details::now(); }

        private:
            std::shared_ptr<state_t> m_state = std::make_shared<state_t>();
        };

        static rpp::schedulers::worker<worker_strategy> create_worker()
        {
            return rpp::schedulers::worker<worker_strategy>{};
        }
    };
} // namespace rpp::schedulers
