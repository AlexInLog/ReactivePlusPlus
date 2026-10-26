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

#include <rpp/schedulers/details/worker.hpp> // worker

#include <rppqt/schedulers/fwd.hpp> // own forwarding
#include <rppqt/utils/exceptions.hpp>

#include "rpp/schedulers/fwd.hpp"

#include <QCoreApplication>
#include <QTimer>
#include <chrono>
#include <concepts>

namespace rppqt::schedulers
{
    /**
     * @brief Schedule provided schedulables to main GUI QT thread (where QApplication placed)
     * @ingroup qt_schedulers
     */
    class main_thread_scheduler final
    {
    private:
        class worker_strategy
        {
        public:
            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, rpp::schedulers::constraint::schedulable_fn<Handler, Args...> Fn>
            static void defer_for(rpp::schedulers::duration duration, Fn&& fn, Handler&& handler, Args&&... args)
            {
                const auto application = QCoreApplication::instance();
                if (!application)
                {
                    handler.on_error(std::make_exception_ptr(utils::no_active_qapplication{"Pointer to application is null. Create QApplication before using main_thread_scheduler!"}));
                    return;
                }

                QTimer::singleShot(std::chrono::duration_cast<std::chrono::milliseconds>(std::max(rpp::schedulers::duration{}, duration)), application, [fn = std::forward<Fn>(fn), handler = std::forward<Handler>(handler), ... args = std::forward<Args>(args)]() mutable {
                    if (!handler.is_disposed())
                        invoke(std::move(fn), std::move(handler), std::move(args)...);
                });
            }

            static rpp::schedulers::time_point now() { return rpp::schedulers::clock_type::now(); }

        private:
            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, rpp::schedulers::constraint::schedulable_delay_from_now_fn<Handler, Args...> Fn>
            static void invoke(Fn&& fn, Handler&& handler, Args&&... args)
            {
                if (const auto new_duration = fn(handler, args...))
                    defer_for(new_duration->value, std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
            }

            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, rpp::schedulers::constraint::schedulable_delay_from_this_timepoint_fn<Handler, Args...> Fn>
            static void invoke(Fn&& fn, Handler&& handler, Args&&... args)
            {
                const auto now = rpp::schedulers::clock_type::now();
                if (const auto new_duration = fn(handler, args...))
                    defer_for(now + new_duration->value - rpp::schedulers::clock_type::now(), std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
            }

            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, rpp::schedulers::constraint::schedulable_delay_to_fn<Handler, Args...> Fn>
            static void invoke(Fn&& fn, Handler&& handler, Args&&... args)
            {
                if (const auto new_tp = fn(handler, args...))
                    defer_for(new_tp->value - rpp::schedulers::clock_type::now(), std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
            }
        };

    public:
        static auto create_worker()
        {
            return rpp::schedulers::worker<worker_strategy>{};
        }
    };
} // namespace rppqt::schedulers
