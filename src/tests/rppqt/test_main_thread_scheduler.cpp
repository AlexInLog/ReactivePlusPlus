//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2022 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#include <doctest/doctest.h>

#include <rpp/observers/dynamic_observer.hpp>
#include <rpp/observers/mock_observer.hpp>

#include <rppqt/schedulers/main_thread.hpp>

#include "rpp/disposables/fwd.hpp"
#include "rpp/schedulers/fwd.hpp"

#include <QApplication>
#include <future>
#include <optional>
#include <thread>

TEST_CASE("main_thread_scheduler schedules actions to main thread")
{
    auto d        = rpp::composite_disposable_wrapper::make();
    auto observer = mock_observer_strategy<int>{}.get_observer(d).as_dynamic();

    int              argc{};
    QCoreApplication application{argc, nullptr};


    SUBCASE("submitting action to main scheduler from another thread")
    {
        // exit on completion instead of a fixed deadline: slow setup could let the deadline expire before the action runs
        QTimer::singleShot(std::chrono::seconds{5}, &application, [&] { application.exit(); });
        std::promise<std::thread::id> execution_thread{};
        std::thread{[&] {
            rppqt::schedulers::main_thread_scheduler::create_worker().schedule([&](const auto&) -> rpp::schedulers::optional_delay_from_now {
                execution_thread.set_value(std::this_thread::get_id());
                application.exit();
                return {};
            },
                                                                               observer);
        }}.join();

        application.exec();
        SUBCASE("thread of exectuion of schedulable should be same as thread of application")
        {
            auto future = execution_thread.get_future();
            REQUIRE(future.wait_for(std::chrono::seconds{1}) == std::future_status::ready);
            CHECK((future.get() == std::this_thread::get_id()));
        }
    }

    SUBCASE("nothing happens for disposed handler")
    {
        QTimer::singleShot(10, &application, [&] { application.exit(); });
        std::promise<std::thread::id> execution_thread{};
        std::thread{[&] {
            rppqt::schedulers::main_thread_scheduler::create_worker().schedule([&](const auto&) -> rpp::schedulers::optional_delay_from_now {
                execution_thread.set_value(std::this_thread::get_id());
                return {};
            },
                                                                               observer);
        }}.join();

        d.dispose();

        application.exec();
        auto future = execution_thread.get_future();
        REQUIRE(future.wait_for(std::chrono::seconds{1}) == std::future_status::timeout);
    }

    auto test_recursive_scheduling = [&](const auto& duration) {
        SUBCASE("recursive scheduling to main thread")
        {
            // exit on completion instead of a fixed deadline: slow setup could let the deadline expire mid-chain
            QTimer::singleShot(std::chrono::seconds{5}, &application, [&] { application.exit(); });
            std::string execution{};
            std::thread{[&] {
                rppqt::schedulers::main_thread_scheduler::create_worker().schedule([&](const auto&) {
                    rppqt::schedulers::main_thread_scheduler::create_worker().schedule([&](const auto&) -> rpp::schedulers::optional_delay_from_now {
                        execution += "inner ";
                        if (execution == "outer inner outer inner ")
                            QTimer::singleShot(10, &application, [&] { application.exit(); });
                        return {};
                    },
                                                                                       observer);

                    const bool first_run = execution.empty();
                    execution += "outer ";
                    return first_run ? duration : std::nullopt;
                },
                                                                                   observer);
            }}.join();

            application.exec();
            CHECK(execution == "outer inner outer inner ");
        }
    };
    SUBCASE("optional_delay_from_now")
    {
        test_recursive_scheduling(rpp::schedulers::optional_delay_from_now{std::chrono::nanoseconds{}});
    }
    SUBCASE("optional_delay_from_this_timepoint")
    {
        test_recursive_scheduling(rpp::schedulers::optional_delay_from_this_timepoint{std::chrono::nanoseconds{}});
    }
    SUBCASE("optional_delay_to")
    {
        test_recursive_scheduling(rpp::schedulers::optional_delay_to{rpp::schedulers::time_point{}});
    }
}

TEST_CASE("no application")
{
    mock_observer_strategy<int> mock{};
    rppqt::schedulers::main_thread_scheduler::create_worker().schedule([&](const auto&) -> rpp::schedulers::optional_delay_from_now {
        return {};
    },
                                                                       mock);

    CHECK(mock.get_on_error_count() == 1);
}
