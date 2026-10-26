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

#include <rpp/disposables/callback_disposable.hpp>
#include <rpp/observers/dynamic_observer.hpp>
#include <rpp/observers/lambda_observer.hpp>
#include <rpp/observers/mock_observer.hpp>
#include <rpp/operators/as_blocking.hpp>
#include <rpp/operators/subscribe_on.hpp>
#include <rpp/schedulers.hpp>
#include <rpp/schedulers/test_scheduler.hpp>
#include <rpp/sources/create.hpp>
#include <rpp/sources/just.hpp>

#include "rpp/disposables/fwd.hpp"
#include "rpp_trompeloil.hpp"

#include <chrono>
#include <future>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

using namespace std::string_literals;

static std::string get_thread_id_as_string(std::thread::id id = std::this_thread::get_id())
{
    std::stringstream ss;
    ss << id;
    return ss.str();
}

static std::string simulate_nested_scheduling(auto worker, const auto& obs, std::vector<std::string>& out)
{
    std::thread thread([&, worker] {
        worker.schedule([&, worker](const auto&) {
            out.push_back("Task 1 starts "s + get_thread_id_as_string());

            worker.schedule([&, worker](const auto&) {
                out.push_back("Task 2 starts "s + get_thread_id_as_string());

                worker.schedule([&](const auto&) {
                    out.push_back("Task 3 runs "s + get_thread_id_as_string());
                    return rpp::schedulers::optional_delay_from_now{};
                },
                                obs);

                out.push_back("Task 2 ends "s + get_thread_id_as_string());
                return rpp::schedulers::optional_delay_from_now{};
            },
                            obs);

            out.push_back("Task 1 ends "s + get_thread_id_as_string());
            return rpp::schedulers::optional_delay_from_now{};
        },
                        obs);
    });

    auto threadid = get_thread_id_as_string(thread.get_id());
    thread.join();
    return threadid;
}

static std::string simulate_complex_scheduling(const auto& worker, const auto& obs, std::vector<std::string>& out)
{
    std::thread thread([&, worker] {
        worker.schedule([&, worker](const auto&) {
            out.push_back("Task 1 starts "s + get_thread_id_as_string());

            worker.schedule([&, worker](const auto&, int& counter) -> rpp::schedulers::optional_delay_from_now {
                out.push_back("Task 2 starts "s + get_thread_id_as_string());

                worker.schedule([&](const auto&) {
                    out.push_back("Task 4 runs "s + get_thread_id_as_string());
                    return rpp::schedulers::optional_delay_from_now{};
                },
                                obs);

                out.push_back("Task 2 ends "s + get_thread_id_as_string());
                if (counter++ < 1)
                    return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
                return std::nullopt;
            },
                            obs,
                            int{});

            worker.schedule([&](const auto&, int& counter) -> rpp::schedulers::optional_delay_from_now {
                out.push_back("Task 3 starts "s + get_thread_id_as_string());

                out.push_back("Task 3 ends "s + get_thread_id_as_string());
                if (counter++ < 1)
                    return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
                return std::nullopt;
            },
                            obs,
                            int{});

            out.push_back("Task 1 ends "s + get_thread_id_as_string());
            return rpp::schedulers::optional_delay_from_now{};
        },
                        obs);
    });

    auto threadid = get_thread_id_as_string(thread.get_id());
    thread.join();
    return threadid;
}

static std::string simulate_complex_scheduling_with_delay(const auto& worker, const auto& obs, std::vector<std::string>& out)
{
    std::thread thread([&, worker] {
        worker.schedule([&, worker](const auto&) {
            out.push_back("Task 1 starts "s + get_thread_id_as_string());

            worker.schedule([&, worker](const auto&, int& counter) -> rpp::schedulers::optional_delay_from_now {
                out.push_back("Task 2 starts "s + get_thread_id_as_string());

                worker.schedule(
                    std::chrono::milliseconds{50},
                    [&](const auto&) {
                        out.push_back("Task 4 runs "s + get_thread_id_as_string());
                        return rpp::schedulers::optional_delay_from_now{};
                    },
                    obs);

                out.push_back("Task 2 ends "s + get_thread_id_as_string());
                if (counter++ < 1)
                    return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
                return std::nullopt;
            },
                            obs,
                            int{});

            worker.schedule([&](const auto&, int& counter) -> rpp::schedulers::optional_delay_from_now {
                out.push_back("Task 3 starts "s + get_thread_id_as_string());

                out.push_back("Task 3 ends "s + get_thread_id_as_string());
                if (counter++ < 1)
                    return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
                return std::nullopt;
            },
                            obs,
                            int{});

            out.push_back("Task 1 ends "s + get_thread_id_as_string());
            return rpp::schedulers::optional_delay_from_now{};
        },
                        obs);
    });

    auto threadid = get_thread_id_as_string(thread.get_id());
    thread.join();
    return threadid;
}

TEST_CASE("Immediate scheduler")
{
    auto scheduler = rpp::schedulers::immediate{};
    auto d         = rpp::composite_disposable_wrapper::make();
    auto mock_obs  = mock_observer_strategy<int>{};
    auto obs       = mock_obs.get_observer(d).as_dynamic();

    auto worker = scheduler.create_worker();

    size_t call_count{};

    SUBCASE("immediate scheduler schedules and re-schedules action immediately")
    {
        worker.schedule([&call_count](const auto&) -> rpp::schedulers::optional_delay_from_now {
            if (++call_count <= 1)
                return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
            return {};
        },
                        obs);

        CHECK(call_count == 2);
    }

    SUBCASE("immediate scheduler schedules action with delay")
    {
        auto now  = rpp::schedulers::clock_type::now();
        auto diff = std::chrono::milliseconds{500};

        rpp::schedulers::time_point execute_time{};
        worker.schedule(
            diff,
            [&call_count, &execute_time](const auto&) -> rpp::schedulers::optional_delay_from_now {
                ++call_count;
                execute_time = rpp::schedulers::clock_type::now();
                return {};
            },
            obs);
        REQUIRE(call_count == 1);
        REQUIRE(execute_time - now >= diff);
    }

    SUBCASE("immediate scheduler re-schedules action at provided timepoint with duration")
    {
        std::vector<rpp::schedulers::time_point> executions{};
        std::chrono::milliseconds                diff = std::chrono::milliseconds{500};
        worker.schedule([&call_count, &executions, &diff](const auto&) -> rpp::schedulers::optional_delay_from_now {
            executions.push_back(rpp::schedulers::clock_type::now());
            if (++call_count <= 1)
                return rpp::schedulers::optional_delay_from_now{diff};
            return {};
        },
                        obs);

        REQUIRE(call_count == 2);
        REQUIRE(executions[1] - executions[0] >= (diff - std::chrono::milliseconds(100)));
    }

    SUBCASE("immediate scheduler re-schedules action at provided timepoint with duration from this")
    {
        std::vector<rpp::schedulers::time_point> executions{};
        std::chrono::milliseconds                diff = std::chrono::milliseconds{500};
        worker.schedule([&call_count, &executions, &diff](const auto&) -> rpp::schedulers::optional_delay_from_this_timepoint {
            executions.push_back(rpp::schedulers::clock_type::now());
            if (++call_count <= 1)
                return rpp::schedulers::optional_delay_from_this_timepoint{diff};
            return {};
        },
                        obs);

        REQUIRE(call_count == 2);
        REQUIRE(executions[1] - executions[0] >= (diff - std::chrono::milliseconds(100)));
    }

    SUBCASE("immediate scheduler re-schedules action at provided timepoint")
    {
        std::vector<rpp::schedulers::time_point> executions{};
        std::chrono::milliseconds                diff = std::chrono::milliseconds{500};
        worker.schedule([&call_count, &executions, &diff](const auto&) -> rpp::schedulers::optional_delay_to {
            executions.push_back(rpp::schedulers::clock_type::now());
            if (++call_count <= 1)
                return rpp::schedulers::optional_delay_to{rpp::schedulers::clock_type::now() + diff};
            return {};
        },
                        obs);

        REQUIRE(call_count == 2);
        REQUIRE(executions[1] - executions[0] >= (diff - std::chrono::milliseconds(100)));
    }

    SUBCASE("immediate scheduler with nesting scheduling should be like call-stack in a recursive order")
    {
        std::vector<std::string> call_stack;

        auto execution_thread = simulate_nested_scheduling(worker, obs, call_stack);

        REQUIRE(call_stack == std::vector<std::string>{
                    "Task 1 starts "s + execution_thread,
                    "Task 2 starts "s + execution_thread,
                    "Task 3 runs "s + execution_thread,
                    "Task 2 ends "s + execution_thread,
                    "Task 1 ends "s + execution_thread,
                });
    }

    SUBCASE("immediate scheduler with complex scheduling with delay should be like call-stack in a recursive order")
    {
        std::vector<std::string> call_stack;

        auto execution_thread = simulate_complex_scheduling_with_delay(worker, obs, call_stack);

        REQUIRE(call_stack == std::vector<std::string>{
                    "Task 1 starts "s + execution_thread,
                    "Task 2 starts "s + execution_thread,
                    "Task 4 runs "s + execution_thread,
                    "Task 2 ends "s + execution_thread,
                    "Task 2 starts "s + execution_thread,
                    "Task 4 runs "s + execution_thread,
                    "Task 2 ends "s + execution_thread,
                    "Task 3 starts "s + execution_thread,
                    "Task 3 ends "s + execution_thread,
                    "Task 3 starts "s + execution_thread,
                    "Task 3 ends "s + execution_thread,
                    "Task 1 ends "s + execution_thread,
                });
    }
    SUBCASE("immediate scheduler with complex scheduling should be like call-stack in a recursive order")
    {
        std::vector<std::string> call_stack;

        auto execution_thread = simulate_complex_scheduling(worker, obs, call_stack);

        REQUIRE(call_stack == std::vector<std::string>{
                    "Task 1 starts "s + execution_thread,
                    "Task 2 starts "s + execution_thread,
                    "Task 4 runs "s + execution_thread,
                    "Task 2 ends "s + execution_thread,
                    "Task 2 starts "s + execution_thread,
                    "Task 4 runs "s + execution_thread,
                    "Task 2 ends "s + execution_thread,
                    "Task 3 starts "s + execution_thread,
                    "Task 3 ends "s + execution_thread,
                    "Task 3 starts "s + execution_thread,
                    "Task 3 ends "s + execution_thread,
                    "Task 1 ends "s + execution_thread,
                });
    }

    SUBCASE("immediate scheduler does nothing with disposed observer")
    {
        d.dispose();
        worker.schedule([&call_count](const auto&) -> rpp::schedulers::optional_delay_from_now {
            ++call_count;
            return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
        },
                        obs);

        CHECK(call_count == 0);
    }

    SUBCASE("immediate scheduler does nothing with observer disposed during wait")
    {
        worker.schedule(
            [&call_count, obs](const auto&) -> rpp::schedulers::optional_delay_from_now {
                ++call_count;
                std::thread{[obs]() {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    obs.on_completed();
                }}
                    .detach();
                return rpp::schedulers::optional_delay_from_now{std::chrono::milliseconds{200}};
            },
            obs);

        CHECK(call_count == 1);
    }

    SUBCASE("immediate scheduler does not reschedule after disposing inside schedulable")
    {
        worker.schedule([&call_count, &d](const auto&) -> rpp::schedulers::optional_delay_from_now {
            if (++call_count > 1)
                d.dispose();
            return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
        },
                        obs);

        CHECK(call_count == 2);
    }

    SUBCASE("immediate scheduler forwards any arguments")
    {
        worker.schedule([](const auto&, int, const std::string&) { return rpp::schedulers::optional_delay_from_now{}; }, obs, int{}, std::string{});
    }

    SUBCASE("error during schedulable")
    {
        worker.schedule([](const auto&) -> rpp::schedulers::optional_delay_from_now { throw std::runtime_error{"test"}; }, obs);
        CHECK(mock_obs.get_on_error_count() == 1);
    }
}

TEST_CASE_TEMPLATE("queue_based scheduler", TestType, rpp::schedulers::current_thread, rpp::schedulers::new_thread, rpp::schedulers::thread_pool)
{
    auto d        = rpp::composite_disposable_wrapper::make();
    auto mock_obs = mock_observer_strategy<int>{};
    auto obs      = std::optional{mock_obs.get_observer(d).as_dynamic()};

    auto   worker = std::optional{TestType{}.create_worker()};
    size_t call_count{};

    std::promise<std::string> thread_of_schedule_promise{};

    auto done = std::make_shared<std::atomic_bool>();

    worker->schedule([done, &thread_of_schedule_promise](const auto&) {
        thread_of_schedule_promise.set_value(get_thread_id_as_string(std::this_thread::get_id()));
        if constexpr (!std::same_as<TestType, rpp::schedulers::current_thread>)
            thread_local rpp::utils::finally_action s_a{[done] {
                done->store(true);
            }};
        else
            done->store(true);

        return rpp::schedulers::optional_delay_from_now{};
    },
                     obs.value());

    auto thread_of_execution = thread_of_schedule_promise.get_future().get();

    auto get_thread = [&]([[maybe_unused]] const std::string& thread_of_schedule) {
        if constexpr (std::same_as<TestType, rpp::schedulers::current_thread>)
            return thread_of_schedule;
        else
            return thread_of_execution;
    };

    auto wait_till_finished = [&] {
        worker.reset();
        obs.reset();
        d = rpp::composite_disposable_wrapper::empty();

        while (!done->load())
        {
        };
    };

    SUBCASE("scheduler schedules and re-schedules action immediately")
    {
        worker->schedule([&call_count](const auto&) -> rpp::schedulers::optional_delay_from_now {
            if (++call_count <= 1)
                return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
            return std::nullopt;
        },
                         obs.value());

        wait_till_finished();

        CHECK(call_count == 2);
    }

    SUBCASE("scheduler recursive scheduling")
    {
        worker->schedule([&call_count, worker](const auto& obs) -> rpp::schedulers::optional_delay_from_now {
            worker->schedule([&call_count](const auto&) -> rpp::schedulers::optional_delay_from_now {
                if (++call_count <= 1)
                    return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
                return std::nullopt;
            },
                             obs);
            return std::nullopt;
        },
                         obs.value());

        wait_till_finished();

        CHECK(call_count == 2);
    }

    SUBCASE("scheduler recursive scheduling with original")
    {
        worker->schedule([&call_count, worker](const auto& obs) -> rpp::schedulers::optional_delay_from_now {
            worker->schedule([&call_count](const auto&) -> rpp::schedulers::optional_delay_from_now {
                if (++call_count <= 1)
                    return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
                return std::nullopt;
            },
                             obs);

            if (call_count == 0)
                return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
            return std::nullopt;
        },
                         obs.value());

        wait_till_finished();

        CHECK(call_count == 3);
    }

    SUBCASE("scheduler schedules action with delay")
    {
        auto now  = rpp::schedulers::clock_type::now();
        auto diff = std::chrono::milliseconds{500};

        rpp::schedulers::time_point execute_time{};
        worker->schedule(
            diff,
            [&call_count, &execute_time](const auto&) -> rpp::schedulers::optional_delay_from_now {
                ++call_count;
                execute_time = rpp::schedulers::clock_type::now();
                return {};
            },
            obs.value());

        wait_till_finished();

        REQUIRE(call_count == 1);
        REQUIRE(execute_time - now >= diff);
    }

    SUBCASE("scheduler re-schedules action at provided timepoint")
    {
        std::vector<rpp::schedulers::time_point> executions{};
        std::chrono::milliseconds                diff = std::chrono::milliseconds{500};
        worker->schedule([&call_count, &executions, &diff](const auto&) -> rpp::schedulers::optional_delay_from_now {
            executions.push_back(rpp::schedulers::clock_type::now());
            if (++call_count <= 1)
                return rpp::schedulers::optional_delay_from_now{diff};
            return {};
        },
                         obs.value());

        wait_till_finished();

        REQUIRE(call_count == 2);
        REQUIRE(executions[1] - executions[0] >= (diff - std::chrono::milliseconds(100)));
    }

    SUBCASE("scheduler re-schedules action at provided timepoint via delay_from_this_timepoint")
    {
        std::vector<rpp::schedulers::time_point> executions{};
        std::chrono::milliseconds                diff = std::chrono::milliseconds{500};
        worker->schedule([&call_count, &executions, &diff](const auto&) -> rpp::schedulers::optional_delay_from_this_timepoint {
            executions.push_back(rpp::schedulers::clock_type::now());
            if (++call_count <= 1)
                return rpp::schedulers::delay_from_this_timepoint{diff};
            return {};
        },
                         obs.value());

        wait_till_finished();

        REQUIRE(call_count == 2);
        REQUIRE(executions[1] - executions[0] >= (diff - std::chrono::milliseconds(100)));
    }

    SUBCASE("scheduler re-schedules action at provided timepoint via delay_to")
    {
        std::vector<rpp::schedulers::time_point> executions{};
        std::chrono::milliseconds                diff = std::chrono::milliseconds{500};
        worker->schedule([&call_count, &executions, &diff](const auto&) -> rpp::schedulers::optional_delay_to {
            executions.push_back(rpp::schedulers::clock_type::now());
            if (++call_count <= 1)
                return rpp::schedulers::delay_to{executions[0] + diff};
            return {};
        },
                         obs.value());

        wait_till_finished();

        REQUIRE(call_count == 2);
        REQUIRE(executions[1] - executions[0] >= (diff - std::chrono::milliseconds(100)));
    }

    SUBCASE("scheduler with nesting scheduling should defer actual execution of tasks")
    {
        std::vector<std::string> call_stack;

        auto execution_thread = get_thread(simulate_nested_scheduling(worker.value(), obs.value(), call_stack));

        wait_till_finished();

        REQUIRE(call_stack == std::vector<std::string>{
                    "Task 1 starts "s + execution_thread,
                    "Task 1 ends "s + execution_thread,
                    "Task 2 starts "s + execution_thread,
                    "Task 2 ends "s + execution_thread,
                    "Task 3 runs "s + execution_thread,
                });
    }

    SUBCASE("scheduler with complex scheduling should defer actual execution of tasks")
    {
        std::vector<std::string> call_stack;

        auto execution_thread = get_thread(simulate_complex_scheduling(worker.value(), obs.value(), call_stack));

        wait_till_finished();

        REQUIRE(call_stack == std::vector<std::string>{
                    "Task 1 starts "s + execution_thread,
                    "Task 1 ends "s + execution_thread,
                    "Task 2 starts "s + execution_thread,
                    "Task 2 ends "s + execution_thread,
                    "Task 3 starts "s + execution_thread,
                    "Task 3 ends "s + execution_thread,
                    "Task 4 runs "s + execution_thread,
                    "Task 2 starts "s + execution_thread,
                    "Task 2 ends "s + execution_thread,
                    "Task 3 starts "s + execution_thread,
                    "Task 3 ends "s + execution_thread,
                    "Task 4 runs "s + execution_thread,
                });
    }

    SUBCASE("scheduler with complex scheduling with delay should defer actual execution of tasks")
    {
        std::vector<std::string> call_stack;

        auto execution_thread = get_thread(simulate_complex_scheduling_with_delay(worker.value(), obs.value(), call_stack));

        wait_till_finished();

        REQUIRE(call_stack == std::vector<std::string>{
                    "Task 1 starts "s + execution_thread,
                    "Task 1 ends "s + execution_thread,
                    "Task 2 starts "s + execution_thread,
                    "Task 2 ends "s + execution_thread,
                    "Task 3 starts "s + execution_thread,
                    "Task 3 ends "s + execution_thread,
                    "Task 2 starts "s + execution_thread,
                    "Task 2 ends "s + execution_thread,
                    "Task 3 starts "s + execution_thread,
                    "Task 3 ends "s + execution_thread,
                    "Task 4 runs "s + execution_thread,
                    "Task 4 runs "s + execution_thread,
                });
    }

    SUBCASE("scheduler does nothing with disposed observer")
    {
        d.dispose();
        worker->schedule([&call_count](const auto&) -> rpp::schedulers::optional_delay_from_now {
            ++call_count;
            return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
        },
                         obs.value());

        wait_till_finished();

        CHECK(call_count == 0);
    }

    SUBCASE("scheduler does nothing with recursive disposed observer")
    {
        worker->schedule([&call_count, d, worker](const auto& obs) -> rpp::schedulers::optional_delay_from_now {
            d.dispose();
            worker->schedule([&call_count](const auto&) -> rpp::schedulers::optional_delay_from_now {
                ++call_count;
                return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
            },
                             obs);

            return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
        },
                         obs.value());

        wait_till_finished();

        CHECK(call_count == 0);
    }

    SUBCASE("scheduler does not reschedule after disposing inside schedulable")
    {
        worker->schedule([&call_count, d](const auto&) -> rpp::schedulers::optional_delay_from_now {
            if (++call_count > 1)
                d.dispose();
            return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
        },
                         obs.value());

        wait_till_finished();

        CHECK(call_count == 2);
    }

    SUBCASE("scheduler does not reschedule after disposing inside recursive schedulable")
    {
        worker->schedule([&call_count, d, worker](const auto& obs) -> rpp::schedulers::optional_delay_from_now {
            worker->schedule([&call_count, d](const auto&) -> rpp::schedulers::optional_delay_from_now {
                if (++call_count > 1)
                    d.dispose();
                return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
            },
                             obs);
            return std::nullopt;
        },
                         obs.value());

        wait_till_finished();

        CHECK(call_count == 2);
    }

    SUBCASE("scheduler does not reschedule after disposing inside recursive schedulable")
    {
        worker->schedule([&call_count, d, worker](const auto& obs) -> rpp::schedulers::optional_delay_from_now {
            worker->schedule([&call_count, d](const auto&) -> rpp::schedulers::optional_delay_from_now {
                if (++call_count > 1)
                    d.dispose();
                return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
            },
                             obs);
            return std::nullopt;
        },
                         obs.value());

        wait_till_finished();

        CHECK(call_count == 2);
    }

    SUBCASE("scheduler does not dispatch schedulable after disposing of disposable")
    {
        worker->schedule([&call_count, d, worker](const auto& obs) -> rpp::schedulers::optional_delay_from_now {
            ++call_count;
            worker->schedule([&call_count](const auto&) -> rpp::schedulers::optional_delay_from_now {
                ++call_count;
                return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
            },
                             obs);
            d.dispose();
            return rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{1}};
        },
                         obs.value());

        wait_till_finished();

        CHECK(call_count == 1);
    }

    SUBCASE("scheduler respects to time point")
    {
        std::vector<int> executions{};
        worker->schedule([&executions, worker](const auto& obs) -> rpp::schedulers::optional_delay_from_now {
            worker->schedule(
                std::chrono::milliseconds{3},
                [&executions](const auto&) {executions.push_back(3); return rpp::schedulers::optional_delay_from_now{}; },
                obs);
            worker->schedule(
                std::chrono::milliseconds{1},
                [&executions](const auto&) {executions.push_back(1); return rpp::schedulers::optional_delay_from_now{}; },
                obs);
            worker->schedule(
                std::chrono::milliseconds{2},
                [&executions](const auto&) {executions.push_back(2); return rpp::schedulers::optional_delay_from_now{}; },
                obs);
            return rpp::schedulers::optional_delay_from_now{};
        },
                         obs.value());

        wait_till_finished();

        CHECK(executions == std::vector{1, 2, 3});
    }

    SUBCASE("scheduler forwards any arguments")
    {
        worker->schedule([](const auto&, int, const std::string&) { return rpp::schedulers::optional_delay_from_now{}; }, obs.value(), int{}, std::string{});
    }

    SUBCASE("error during schedulable")
    {
        worker->schedule([](const auto&) -> rpp::schedulers::optional_delay_from_now { throw std::runtime_error{"test"}; }, obs.value());

        wait_till_finished();

        CHECK(mock_obs.get_on_error_count() == 1);
    }

    SUBCASE("error during recursive schedulable")
    {
        worker->schedule([worker](const auto& obs) {
            worker->schedule([](const auto&) -> rpp::schedulers::optional_delay_from_now { throw std::runtime_error{"test"}; }, obs);
            return rpp::schedulers::optional_delay_from_now{};
        },
                         obs.value());

        wait_till_finished();

        CHECK(mock_obs.get_on_error_count() == 1);
    }
}

TEST_CASE("new_thread utilized current_thread")
{
    std::atomic_bool inner_schedule_executed{};
    auto             mock = mock_observer_strategy<int>{};
    {
        auto worker = rpp::schedulers::new_thread::create_worker();
        auto obs    = mock.get_observer().as_dynamic();
        worker.schedule([&inner_schedule_executed](const auto& obs) {
            rpp::schedulers::current_thread::create_worker().schedule([&inner_schedule_executed](const auto&) {
                inner_schedule_executed = true;
                return rpp::schedulers::optional_delay_from_now{};
            },
                                                                      obs);

            if (inner_schedule_executed)
                throw std::logic_error{"current_thread executed inside new_thread"};
            return rpp::schedulers::optional_delay_from_now{};
        },
                        obs);
    }

    while (!inner_schedule_executed)
    {
    };

    CHECK(inner_schedule_executed);
    CHECK(mock.get_on_error_count() == 0);
}

TEST_CASE("new_thread works till end")
{
    auto                  mock = mock_observer<int>{};
    trompeloeil::sequence s{};

    const auto vals = std::array{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    auto checked = std::make_shared<std::atomic_bool>();

    REQUIRE_CALL(*mock, on_next_lvalue(trompeloeil::_)).TIMES(10).IN_SEQUENCE(s);

    auto done = std::make_shared<std::atomic_bool>();

    const auto last = NAMED_REQUIRE_CALL(*mock, on_completed()).LR_SIDE_EFFECT({
                                                                   thread_local rpp::utils::finally_action s_a{[done] {
                                                                       done->store(true);
                                                                   }};
                                                               })
                          .IN_SEQUENCE(s);

    // gate emission outside of the mock: trompeloeil holds its global lock while running side effects
    rpp::source::create<int>([checked, vals](const auto& obs) {
        while (!checked->load())
        {
        };
        for (const auto v : vals)
            obs.on_next(v);
        obs.on_completed();
    })
        | rpp::operators::subscribe_on(rpp::schedulers::new_thread{})
        | rpp::operators::subscribe(mock);

    const bool before = last->is_satisfied();
    checked->store(true);

    wait(last);

    while (!done->load())
    {
    };

    CHECK(!before);
}

TEST_CASE("run_loop scheduler dispatches tasks only manually")
{
    auto scheduler = rpp::schedulers::run_loop{};
    auto worker    = scheduler.create_worker();
    auto d         = rpp::composite_disposable_wrapper::make();
    auto obs       = mock_observer_strategy<int>{}.get_observer(d).as_dynamic();

    SUBCASE("submit 3 tasks to run_loop")
    {
        size_t schedulable_1_executed_count{};
        size_t schedulable_2_executed_count{};
        size_t schedulable_3_executed_count{};
        worker.schedule([&](const auto&) -> rpp::schedulers::optional_delay_from_now {++schedulable_1_executed_count; return {}; }, obs);
        worker.schedule([&](const auto&) -> rpp::schedulers::optional_delay_from_now {++schedulable_2_executed_count; d.dispose(); return {}; }, obs);
        worker.schedule([&](const auto&) -> rpp::schedulers::optional_delay_from_now {++schedulable_3_executed_count; return {}; }, obs);

        SUBCASE("nothing happens but scheduler has schedulable to dispatch")
        {
            CHECK(schedulable_1_executed_count == 0);
            CHECK(schedulable_2_executed_count == 0);
            CHECK(schedulable_3_executed_count == 0);
            CHECK(d.is_disposed() == false);
            CHECK(scheduler.is_empty() == false);
            CHECK(scheduler.is_any_ready_schedulable() == true);
        }
        SUBCASE("call dispatch_if_ready")
        {
            scheduler.dispatch_if_ready();
            SUBCASE("only first schedulable dispatched")
            {
                CHECK(schedulable_1_executed_count == 1);
                CHECK(schedulable_2_executed_count == 0);
                CHECK(schedulable_3_executed_count == 0);
                CHECK(d.is_disposed() == false);
                CHECK(scheduler.is_empty() == false);
                CHECK(scheduler.is_any_ready_schedulable() == true);

                SUBCASE("call dispatch_if_ready again")
                {
                    scheduler.dispatch_if_ready();
                    SUBCASE("both schedulable dispatched")
                    {
                        CHECK(schedulable_1_executed_count == 1);
                        CHECK(schedulable_2_executed_count == 1);
                        CHECK(schedulable_3_executed_count == 0);
                        CHECK(d.is_disposed() == true);
                        CHECK(scheduler.is_empty() == false);
                        CHECK(scheduler.is_any_ready_schedulable() == true);
                    }
                    SUBCASE("call dispatch_if_ready again")
                    {
                        scheduler.dispatch_if_ready();
                        SUBCASE("third scehdulable not dispatched, but scheduler is empty")
                        {
                            CHECK(schedulable_1_executed_count == 1);
                            CHECK(schedulable_2_executed_count == 1);
                            CHECK(schedulable_3_executed_count == 0);
                            CHECK(d.is_disposed() == true);
                            CHECK(scheduler.is_empty() == true);
                            CHECK(scheduler.is_any_ready_schedulable() == false);
                        }
                    }
                }
            }
        }
    }
    SUBCASE("submit 1 task to run_loop")
    {
        size_t schedulable_1_executed_count{};
        worker.schedule([&](const auto&) -> rpp::schedulers::optional_delay_from_now {++schedulable_1_executed_count; return {}; }, obs);

        SUBCASE("call dispatch")
        {
            scheduler.dispatch();
            SUBCASE("only first schedulable dispatched")
            {
                CHECK(schedulable_1_executed_count == 1);
                CHECK(d.is_disposed() == false);
                CHECK(scheduler.is_empty() == true);
                CHECK(scheduler.is_any_ready_schedulable() == false);

                SUBCASE("call dispatch and schedule in other thread")
                {
                    std::atomic_bool dispatched{};
                    size_t           schedulable_2_executed_count{};

                    auto t = std::thread{[&] {
                        std::this_thread::sleep_for(std::chrono::milliseconds{100});
                        if (!scheduler.is_empty()) throw std::runtime_error{"!is_empty"};
                        if (scheduler.is_any_ready_schedulable()) throw std::runtime_error{"is_any_ready_schedulable"};
                        if (dispatched) throw std::runtime_error{"dispatched"};

                        worker.schedule(
                            std::chrono::milliseconds{1},
                            [&](const auto&) -> rpp::schedulers::optional_delay_from_now {++schedulable_2_executed_count; return {}; },
                            obs);
                    }};
                    scheduler.dispatch();
                    CHECK(schedulable_2_executed_count == 1);
                    dispatched = true;
                    t.join();
                }
            }
        }
    }
}

TEST_CASE("different delaying strategies")
{
    rpp::schedulers::test_scheduler scheduler{};
    auto                            obs     = mock_observer_strategy<int>{}.get_observer().as_dynamic();
    auto                            advance = std::chrono::seconds{1};
    auto                            delay   = advance * 2;
    auto                            now     = scheduler.now();

    auto test = [&](auto res) {
        scheduler.create_worker().schedule([&, res](const auto&) {
            scheduler.time_advance(advance);
            return res;
        },
                                           obs);
    };

    SUBCASE("return delay_from_now")
    {
        test(rpp::schedulers::optional_delay_from_now{delay});
        CHECK(scheduler.get_schedulings() == std::vector{now, now + advance + delay});
        CHECK(scheduler.get_executions() == std::vector{now});
    }

    SUBCASE("return delay_from_this_timepoint")
    {
        test(rpp::schedulers::optional_delay_from_this_timepoint{delay});
        CHECK(scheduler.get_schedulings() == std::vector{now, now + delay});
        CHECK(scheduler.get_executions() == std::vector{now});
    }

    SUBCASE("return delay_to")
    {
        test(rpp::schedulers::optional_delay_to{now + delay});
        CHECK(scheduler.get_schedulings() == std::vector{now, now + delay});
        CHECK(scheduler.get_executions() == std::vector{now});
    }
}

TEST_CASE("current_thread inside new_thread")
{
    auto worker  = std::optional{rpp::schedulers::new_thread{}.create_worker()};
    auto d       = rpp::composite_disposable_wrapper::make();
    auto obs     = std::optional{mock_observer_strategy<int>{}.get_observer(d).as_dynamic()};
    auto started = std::make_shared<std::atomic_bool>();
    auto done    = std::make_shared<std::atomic_bool>();

    worker->schedule([&](const auto&) {
        thread_local rpp::utils::finally_action s_th{[done] {
            done->store(true);
        }};
        return rpp::schedulers::optional_delay_from_now{};
    },
                     obs.value());

    auto current_thread_invoked = std::make_shared<std::atomic_bool>();

    worker->schedule([&](const auto& obs) {
        rpp::schedulers::current_thread{}.create_worker().schedule([current_thread_invoked](const auto&) {
            current_thread_invoked->store(true);
            return rpp::schedulers::optional_delay_from_now{};
        },
                                                                   obs);

        if (current_thread_invoked->load())
            throw std::runtime_error{"current_thread was invoked"};

        started->store(true);

        return rpp::schedulers::optional_delay_from_now{};
    },
                     obs.value());

    while (!started->load())
    {
    }

    worker.reset();
    obs.reset();
    d = rpp::composite_disposable_wrapper::empty();

    std::this_thread::sleep_for(std::chrono::seconds{1});

    REQUIRE(done->load());
    CHECK(current_thread_invoked->load());
}

TEST_CASE("thread_pool uses multiple threads")
{
    auto obs = mock_observer_strategy<int>{}.get_observer().as_dynamic();

    auto scheduler = rpp::schedulers::thread_pool{3};

    const auto get_thread_id = [&scheduler, &obs]() {
        std::promise<std::thread::id> promise{};
        scheduler.create_worker().schedule([&promise](const auto&) {
            promise.set_value(std::this_thread::get_id());
            return rpp::schedulers::optional_delay_from_now{};
        },
                                           obs);
        return promise.get_future().get();
    };

    const auto thread_1_value = get_thread_id();
    const auto thread_2_value = get_thread_id();
    const auto thread_3_value = get_thread_id();
    CHECK(thread_1_value != thread_2_value);
    CHECK(thread_1_value != thread_3_value);
    CHECK(thread_2_value != thread_3_value);

    CHECK(thread_1_value == get_thread_id());
    CHECK(thread_2_value == get_thread_id());
    CHECK(thread_3_value == get_thread_id());
}


TEST_CASE("thread_pool shares same thread")
{
    auto obs = mock_observer_strategy<int>{}.get_observer().as_dynamic();

    auto scheduler = rpp::schedulers::thread_pool{1};

    std::atomic_bool first_job_done{};

    scheduler.create_worker().schedule([&first_job_done](const auto&) {
        while (!first_job_done)
            std::this_thread::yield();
        return rpp::schedulers::optional_delay_from_now{};
    },
                                       obs);

    std::promise<bool> second_task_executed_promise{};
    scheduler.create_worker().schedule([&second_task_executed_promise](const auto&) {
        second_task_executed_promise.set_value(true);
        return rpp::schedulers::optional_delay_from_now{};
    },
                                       obs);
    auto f = second_task_executed_promise.get_future();

    CHECK(f.wait_for(std::chrono::seconds{1}) == std::future_status::timeout);
    first_job_done.store(true);

    CHECK(f.get());
}

TEST_CASE("timer queued while the worker waits for a later one runs at its own time")
{
    // The worker is already waiting for a schedulable due in `later`. A second one, due
    // in `earlier` (much sooner), is queued from another thread. It has to run after
    // about `earlier`, not when the wait for the first one ends.
    constexpr auto later   = std::chrono::seconds{3};
    constexpr auto earlier = std::chrono::milliseconds{50};
    constexpr auto bound   = std::chrono::seconds{1};

    auto       d    = rpp::composite_disposable_wrapper::make();
    auto       obs  = mock_observer_strategy<int>{}.get_observer(d).as_dynamic();
    const auto noop = [](const auto&) {
        return rpp::schedulers::optional_delay_from_now{};
    };

    SUBCASE("run_loop")
    {
        auto scheduler = rpp::schedulers::run_loop{};
        auto worker    = scheduler.create_worker();
        worker.schedule(later, noop, obs);

        std::atomic_bool                      earlier_ran{};
        std::chrono::steady_clock::time_point queued_at{};
        std::chrono::steady_clock::time_point ran_at{};

        auto other = std::thread{[&] {
            std::this_thread::sleep_for(std::chrono::milliseconds{100}); // dispatch() below waits for `later` by now
            queued_at = std::chrono::steady_clock::now();
            worker.schedule(
                earlier,
                [&](const auto&) {
                    ran_at = std::chrono::steady_clock::now();
                    earlier_ran.store(true);
                    return rpp::schedulers::optional_delay_from_now{};
                },
                obs);
        }};

        while (!earlier_ran.load())
            scheduler.dispatch();
        other.join();

        CHECK(ran_at - queued_at < bound);
    }

    SUBCASE("new_thread")
    {
        auto worker = rpp::schedulers::new_thread::create_worker();
        worker.schedule(later, noop, obs);
        std::this_thread::sleep_for(std::chrono::milliseconds{100}); // the worker thread waits for `later` by now

        std::promise<std::chrono::steady_clock::time_point> ran_at{};
        const auto                                          queued_at = std::chrono::steady_clock::now();
        worker.schedule(
            earlier,
            [&ran_at](const auto&) {
                ran_at.set_value(std::chrono::steady_clock::now());
                return rpp::schedulers::optional_delay_from_now{};
            },
            obs);

        auto f = ran_at.get_future();
        REQUIRE(f.wait_for(later * 2) == std::future_status::ready);
        CHECK(f.get() - queued_at < bound);
    }

    d.dispose();
}
