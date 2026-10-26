//                  ReactivePlusPlus library
//
//          Copyright Aleksey Loginov 2022 - present.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          https://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/AlexInLog/ReactivePlusPlus
//

#pragma once

#include <rpp/schedulers/current_thread.hpp>
#include <rpp/schedulers/details/worker.hpp>

#include <asio/basic_waitable_timer.hpp>
#include <asio/bind_executor.hpp>
#include <asio/strand.hpp>

namespace rppasio::schedulers
{
    /**
     * @brief Asio based scheduler where each worker is assigned an asio `strand` to execute schedulables with the
     * guarantee that none of those `schedulables` will execute concurrently.
     * @details This scheduler can efficiently enable multi-threading execution when running provided io_context
     * in multiple threads. Compared to the `thread_pool` scheduler, a worker is not pinned to a single thread and
     * `schedulables` are instead dynamically dispatched to potentially different io_context threads depending
     * on load.
     * @ingroup asio_schedulers
     */
    class strand
    {
    private:
        class state_t : public std::enable_shared_from_this<state_t>
        {
            class current_thread_queue_guard;

        public:
            state_t(const asio::io_context::executor_type& executor)
                : m_strand{executor.context()}
            {
            }

            template<typename Handler, typename... Args, typename Fn>
            void defer(Fn&& fn, Handler&& handler, Args&&... args) const
            {
                if (handler.is_disposed())
                    return;

                asio::post(asio::bind_executor(m_strand, [self = this->shared_from_this(), fn = std::forward<Fn>(fn), handler = std::forward<Handler>(handler), ... args = std::forward<Args>(args)]() mutable {
                    if (handler.is_disposed())
                        return;

                    current_thread_queue_guard guard{*self};
                    if (const auto new_duration = fn(handler, args...))
                        self->defer_with_time(new_duration->value, std::move(fn), std::move(handler), std::move(args)...);
                }));
            }

            template<typename Time, typename Handler, typename... Args, typename Fn>
            void defer_with_time(Time time, Fn&& fn, Handler&& handler, Args&&... args) const
            {
                if (handler.is_disposed())
                    return;

                auto timer = std::make_shared<asio::basic_waitable_timer<rpp::schedulers::clock_type>>(m_strand.context(), time);
                timer->async_wait(asio::bind_executor(m_strand, [self = this->shared_from_this(), timer, fn = std::forward<Fn>(fn), handler = std::forward<Handler>(handler), ... args = std::forward<Args>(args)](const asio::error_code& ec) mutable {
                    if (ec || handler.is_disposed())
                        return;

                    current_thread_queue_guard guard{*self};
                    if (const auto new_duration = fn(handler, args...))
                        self->defer_with_time(new_duration->value, std::move(fn), std::move(handler), std::move(args)...);
                }));
            }

        private:
            // Guard draining schedulables queued to thread local queue to schedule them back to strand queue
            class current_thread_queue_guard
            {
            public:
                current_thread_queue_guard(const state_t& state)
                    : m_process_on_destruction{!rpp::schedulers::current_thread::get_queue()}
                    , m_state(state)
                {
                    if (m_process_on_destruction)
                        rpp::schedulers::current_thread::get_queue() = &m_queue;
                }
                ~current_thread_queue_guard()
                {
                    if (m_process_on_destruction)
                        process_queue();
                }
                current_thread_queue_guard(const current_thread_queue_guard&) = delete;
                current_thread_queue_guard(current_thread_queue_guard&&)      = delete;

            private:
                struct handler
                {
                    bool is_disposed() const noexcept
                    {
                        return m_schedulable->is_disposed();
                    }

                    void on_error(const std::exception_ptr& ep) const
                    {
                        m_schedulable->on_error(ep);
                    }

                    std::shared_ptr<rpp::schedulers::details::schedulable_base> m_schedulable;
                };

            private:
                void process_queue()
                {
                    while (!m_queue.is_empty())
                    {
                        const auto top = m_queue.pop();
                        if (top->is_disposed())
                            continue;

                        m_state.defer_with_time(
                            top->get_timepoint(),
                            [top](const auto&) -> rpp::schedulers::optional_delay_to {
                                if (const auto advanced_call = top->make_advanced_call())
                                {
                                    const auto tp = top->handle_advanced_call(*advanced_call);
                                    top->set_timepoint(tp);
                                    return rpp::schedulers::delay_to{tp};
                                }
                                return std::nullopt;
                            },
                            handler{top});
                    }
                    rpp::schedulers::current_thread::get_queue() = nullptr;
                }

            private:
                rpp::schedulers::details::schedulables_queue<rpp::schedulers::current_thread::worker_strategy> m_queue;
                bool                                                                                           m_process_on_destruction;
                const state_t&                                                                                 m_state;
            };

        private:
            asio::io_context::strand m_strand;
        };

        class worker_strategy
        {
        public:
            explicit worker_strategy(const asio::io_context::executor_type& executor)
                : m_state{std::make_shared<state_t>(executor)}
            {
            }

            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, rpp::schedulers::constraint::schedulable_fn<Handler, Args...> Fn>
            void defer_for(rpp::schedulers::duration duration, Fn&& fn, Handler&& handler, Args&&... args) const
            {
                if (duration == rpp::schedulers::duration::zero())
                    m_state->defer(std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
                else
                    m_state->defer_with_time(duration, std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
            }

            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, rpp::schedulers::constraint::schedulable_fn<Handler, Args...> Fn>
            void defer_to(rpp::schedulers::time_point tp, Fn&& fn, Handler&& handler, Args&&... args) const
            {
                m_state->defer_with_time(tp, std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
            }

            static rpp::schedulers::time_point now() { return rpp::schedulers::clock_type::now(); }

        private:
            std::shared_ptr<state_t> m_state;
        };

    public:
        explicit strand(asio::io_context::executor_type executor)
            : m_executor{std::move(executor)}
        {
        }

        auto create_worker() const
        {
            return rpp::schedulers::worker<worker_strategy>{m_executor};
        }

        asio::io_context::executor_type m_executor;
    };
} // namespace rppasio::schedulers
