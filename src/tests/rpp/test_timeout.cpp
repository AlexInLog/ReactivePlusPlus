//                  ReactivePlusPlus library
//
//          Copyright Aleksey Loginov 2023 - present.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          https://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/AlexInLog/ReactivePlusPlus
//

#include <doctest/doctest.h>

#include <rpp/observers/mock_observer.hpp>
#include <rpp/operators/delay.hpp>
#include <rpp/operators/timeout.hpp>
#include <rpp/schedulers/test_scheduler.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/just.hpp>
#include <rpp/sources/never.hpp>
#include <rpp/subjects/publish_subject.hpp>

#include "disposable_observable.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace
{
    // wait() blocks until open() has been called once, then closes again.
    class gate
    {
    public:
        void open()
        {
            {
                const std::lock_guard lock{m_mutex};
                m_open = true;
            }
            m_cv.notify_one();
        }

        void wait()
        {
            std::unique_lock lock{m_mutex};
            m_cv.wait(lock, [this] { return m_open; });
            m_open = false;
        }

    private:
        std::mutex              m_mutex{};
        std::condition_variable m_cv{};
        bool                    m_open{};
    };

    // Runs its schedulables only when fire() is called, on the calling thread. On the thread set in
    // s_hold_now_on, the next now() blocks until s_release_now is signalled: the timeout handler
    // calls now() while it holds the operator's lock, so a test can pause it there.
    class manual_scheduler final
    {
    public:
        static inline rpp::schedulers::time_point  s_now{};
        static inline std::atomic<std::thread::id> s_hold_now_on{};
        static inline gate                         s_now_held{};
        static inline gate                         s_release_now{};

        class worker_strategy;

        struct state
        {
            rpp::schedulers::details::schedulables_queue<worker_strategy> queue{};
        };

        class worker_strategy
        {
        public:
            explicit worker_strategy(std::shared_ptr<state> state)
                : m_state{std::move(state)}
            {
            }

            template<rpp::schedulers::constraint::schedulable_handler Handler, typename... Args, rpp::schedulers::constraint::schedulable_fn<Handler, Args...> Fn>
            void defer_to(rpp::schedulers::time_point time_point, Fn&& fn, Handler&& handler, Args&&... args) const
            {
                m_state->queue.emplace(time_point, std::forward<Fn>(fn), std::forward<Handler>(handler), std::forward<Args>(args)...);
            }

            static rpp::schedulers::time_point now()
            {
                if (s_hold_now_on.load() == std::this_thread::get_id())
                {
                    s_hold_now_on.store(std::thread::id{});
                    s_now_held.open();
                    s_release_now.wait();
                }
                return s_now;
            }

        private:
            std::shared_ptr<state> m_state;
        };

        rpp::schedulers::worker<worker_strategy> create_worker() const
        {
            return rpp::schedulers::worker<worker_strategy>{m_state};
        }

        void fire() const
        {
            const auto schedulable = m_state->queue.top();
            m_state->queue.pop();
            if (!schedulable->is_disposed())
                (void)(*schedulable)();
        }

    private:
        std::shared_ptr<state> m_state = std::make_shared<state>();
    };

    // Counts every call that reaches an instance which has been moved from.
    class move_aware_strategy
    {
    public:
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::Auto;

        explicit move_aware_strategy(std::atomic<size_t>& calls_on_moved_from)
            : m_calls_on_moved_from{&calls_on_moved_from}
        {
        }

        move_aware_strategy(move_aware_strategy&& other) noexcept
            : m_calls_on_moved_from{other.m_calls_on_moved_from}
        {
            other.m_moved_from = true;
        }

        void on_next(const int&) const { count(); }
        void on_next(int&&) const { count(); }
        void on_error(const std::exception_ptr&) const { count(); }
        void on_completed() const { count(); }

        static void set_upstream(const rpp::disposable_wrapper&) { }
        static bool is_disposed() { return false; }

    private:
        void count() const
        {
            if (m_moved_from)
                m_calls_on_moved_from->fetch_add(1);
        }

        std::atomic<size_t>* m_calls_on_moved_from;
        bool                 m_moved_from{};
    };
} // namespace


TEST_CASE("timeout subscribes to passed observable in case of reaching timeout")
{
    auto       scheduler = rpp::schedulers::test_scheduler{};
    auto       mock      = mock_observer_strategy<int>{};
    const auto now       = scheduler.now();

    SUBCASE("timeout not reached")
    {
        rpp::source::just(scheduler, 1, 2, 3)
            | rpp::ops::timeout(std::chrono::seconds{1}, rpp::source::just(100), scheduler)
            | rpp::ops::subscribe(mock);

        for (size_t i = 0; i < 3; ++i)
            scheduler.time_advance(std::chrono::seconds{0});
        scheduler.time_advance(std::chrono::seconds{1});

        CHECK(scheduler.get_executions() == std::vector{now, now, now});
        CHECK(scheduler.get_schedulings() == std::vector{now + std::chrono::seconds{1}, now, now, now});
        CHECK(mock.get_received_values() == std::vector{1, 2, 3});
        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
    }

    SUBCASE("timeout reached")
    {
        rpp::source::just(scheduler, 1)
            | rpp::ops::delay(std::chrono::seconds{5}, scheduler)
            | rpp::ops::timeout(std::chrono::seconds{1}, rpp::source::just(100), scheduler)
            | rpp::ops::subscribe(mock);

        scheduler.time_advance(std::chrono::seconds{0});

        for (size_t i = 0; i < 10; ++i)
            scheduler.time_advance(std::chrono::seconds{1});

        CHECK(scheduler.get_executions() == std::vector{now, now + std::chrono::seconds{1}});
        CHECK(scheduler.get_schedulings() == std::vector{now + std::chrono::seconds{1}, now, now + std::chrono::seconds{5}});
        CHECK(mock.get_received_values() == std::vector{100});
        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
    }

    SUBCASE("timeout with never")
    {
        rpp::source::never<int>()
            | rpp::ops::timeout(std::chrono::seconds{1}, rpp::source::just(100), scheduler)
            | rpp::ops::subscribe(mock);

        scheduler.time_advance(std::chrono::seconds{1});

        CHECK(scheduler.get_executions() == std::vector{now + std::chrono::seconds{1}});
        CHECK(scheduler.get_schedulings() == std::vector{now + std::chrono::seconds{1}});
        CHECK(mock.get_received_values() == std::vector{100});
        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
    }

    SUBCASE("timeout with empty")
    {
        rpp::source::empty<int>()
            | rpp::ops::timeout(std::chrono::seconds{1}, rpp::source::just(100), scheduler)
            | rpp::ops::subscribe(mock);

        scheduler.time_advance(std::chrono::seconds{1});

        CHECK(scheduler.get_executions() == std::vector<rpp::schedulers::time_point>{});
        CHECK(scheduler.get_schedulings() == std::vector{now + std::chrono::seconds{1}});
        CHECK(mock.get_received_values() == std::vector<int>{});
        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
    }

    SUBCASE("timeout with error")
    {
        rpp::source::error<int>({})
            | rpp::ops::timeout(std::chrono::seconds{1}, rpp::source::just(100), scheduler)
            | rpp::ops::subscribe(mock);

        scheduler.time_advance(std::chrono::seconds{1});

        CHECK(scheduler.get_executions() == std::vector<rpp::schedulers::time_point>{});
        CHECK(scheduler.get_schedulings() == std::vector{now + std::chrono::seconds{1}});
        CHECK(mock.get_received_values() == std::vector<int>{});
        CHECK(mock.get_on_error_count() == 1);
        CHECK(mock.get_on_completed_count() == 0);
    }

    SUBCASE("timeout with default args")
    {
        rpp::source::never<int>()
            | rpp::ops::timeout(std::chrono::seconds{1}, scheduler)
            | rpp::ops::subscribe(mock);

        scheduler.time_advance(std::chrono::seconds{1});

        CHECK(scheduler.get_executions() == std::vector{now + std::chrono::seconds{1}});
        CHECK(scheduler.get_schedulings() == std::vector{now + std::chrono::seconds{1}});
        CHECK(mock.get_received_values() == std::vector<int>{});
        CHECK(mock.get_on_error_count() == 1);
        CHECK(mock.get_on_completed_count() == 0);
    }

    SUBCASE("never timeout with never")
    {
        rpp::source::never<int>()
            | rpp::ops::timeout(std::chrono::seconds{1}, rpp::source::never<int>(), scheduler)
            | rpp::ops::subscribe(mock);

        scheduler.time_advance(std::chrono::seconds{1});

        CHECK(scheduler.get_executions() == std::vector{now + std::chrono::seconds{1}});
        CHECK(scheduler.get_schedulings() == std::vector{now + std::chrono::seconds{1}});
        CHECK(mock.get_received_values() == std::vector<int>{});
        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 0);
    }
}

TEST_CASE("timeout handles current_thread scheduling")
{
    auto mock = mock_observer_strategy<int>{};

    rpp::source::just(rpp::schedulers::current_thread{}, 1, 2, 3)
        | rpp::operators::timeout(std::chrono::seconds{2}, rpp::source::error<int>({}), rpp::schedulers::current_thread{})
        | rpp::operators::subscribe(mock);

    CHECK(mock.get_received_values() == std::vector{1, 2, 3});
    CHECK(mock.get_on_error_count() == 0);
    CHECK(mock.get_on_completed_count() == 1);
}

TEST_CASE("timeout handles exception from fallback")
{
    auto mock = mock_observer_strategy<int>{};

    rpp::source::never<int>()
        | rpp::operators::timeout(std::chrono::seconds{1}, rpp::source::create<int>([](const auto&) {
                                      throw 1;
                                  }),
                                  rpp::schedulers::current_thread{})
        | rpp::operators::subscribe(mock);

    CHECK(mock.get_received_values() == std::vector<int>{});
    CHECK(mock.get_on_error_count() == 1);
    CHECK(mock.get_on_completed_count() == 0);
}

TEST_CASE("timeout does not pass a value that races the timer to the observer handed to the fallback")
{
    // The timer runs on one thread and a value arrives on another. The value passes the disposed check
    // before the timer disposes, then waits for the lock the timer holds while it hands the observer
    // to the fallback. Once it gets the lock, the value must not reach that moved-from observer.
    const auto scheduler = manual_scheduler{};
    auto       source    = rpp::subjects::publish_subject<int>{};
    auto       fallback  = rpp::subjects::publish_subject<int>{};

    std::atomic<size_t> calls_on_moved_from{};
    source.get_observable()
        | rpp::ops::timeout(std::chrono::seconds{1}, fallback.get_observable(), scheduler)
        | rpp::ops::subscribe(rpp::observer<int, move_aware_strategy>{calls_on_moved_from});

    manual_scheduler::s_now += std::chrono::seconds{1};

    std::thread timer_thread{[&] {
        manual_scheduler::s_hold_now_on.store(std::this_thread::get_id());
        scheduler.fire();
    }};
    manual_scheduler::s_now_held.wait();

    std::thread value_thread{[&] { source.get_observer().on_next(1); }};
    // let the value pass the disposed check and block on the operator's lock
    std::this_thread::sleep_for(std::chrono::milliseconds{200});

    manual_scheduler::s_release_now.open();
    timer_thread.join();
    value_thread.join();

    CHECK(calls_on_moved_from.load() == 0);
}

TEST_CASE("timeout satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::timeout(std::chrono::seconds{10000000}, rpp::schedulers::test_scheduler{}));
}
