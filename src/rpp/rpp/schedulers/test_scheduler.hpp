//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/schedulers.hpp>

namespace rpp::schedulers
{


    class test_scheduler final
    {
    public:
        static inline rpp::schedulers::time_point s_current_time{std::chrono::seconds{10}};

        class worker_strategy;

        struct state
        {
            state() = default;

            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, rpp::schedulers::constraint::schedulable_fn<Handler, Args...> Fn>
            void schedule(rpp::schedulers::time_point time_point, Fn&& fn, Handler&& handler, Args&&... args)
            {
                schedulings.push_back(time_point);
                queue.emplace(time_point, std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
            }

            void drain()
            {
                while (!queue.is_empty())
                {
                    if (queue.top()->get_timepoint() > s_current_time)
                        return;

                    auto fn = queue.top();
                    queue.pop();

                    if (fn->is_disposed())
                        continue;

                    executions.push_back(s_current_time);
                    if (auto new_timepoint = (*fn)())
                    {
                        if (fn->is_disposed())
                            continue;

                        schedulings.push_back(std::max(s_current_time, new_timepoint.value()));
                        queue.emplace(schedulings.back(), std::move(fn));
                    }
                }
            }

            std::vector<rpp::schedulers::time_point>                      schedulings{};
            std::vector<rpp::schedulers::time_point>                      executions{};
            rpp::schedulers::details::schedulables_queue<worker_strategy> queue{};
        };

        class worker_strategy
        {
        public:
            worker_strategy(std::weak_ptr<state> state)
                : m_state{std::move(state)}
            {
            }

            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, rpp::schedulers::constraint::schedulable_fn<Handler, Args...> Fn>
            void defer_for(rpp::schedulers::duration duration, Fn&& fn, Handler&& handler, Args&&... args) const
            {
                if (auto locked = m_state.lock())
                {
                    locked->schedule(now() + duration, std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
                    locked->drain();
                }
            }

            static rpp::schedulers::time_point now() { return s_current_time; }

        private:
            std::weak_ptr<state> m_state;
        };

        test_scheduler() = default;

        rpp::schedulers::worker<worker_strategy> create_worker() const
        {
            return rpp::schedulers::worker<worker_strategy>{m_state};
        }

        const auto& get_schedulings() const { return m_state->schedulings; }
        const auto& get_executions() const { return m_state->executions; }

        static rpp::schedulers::time_point now() { return s_current_time; }

        void time_advance(rpp::schedulers::duration dur) const
        {
            s_current_time += dur;
            m_state->drain();
        }

    private:
        std::shared_ptr<state> m_state = std::make_shared<state>();
    };
} // namespace rpp::schedulers
