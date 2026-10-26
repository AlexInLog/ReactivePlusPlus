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
#include <rpp/operators/on_error_resume_next.hpp>
#include <rpp/schedulers/immediate.hpp>
#include <rpp/sources/create.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/just.hpp>

#include "disposable_observable.hpp"

TEST_CASE_TEMPLATE("on_error_resume_next switches observable on error", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    auto mock = mock_observer_strategy<int>();
    SUBCASE("observable without error emission")
    {
        auto obs = rpp::source::just<TestType>(rpp::schedulers::immediate{}, 1, 2, 3);
        SUBCASE("subscribe")
        {
            obs | rpp::operators::on_error_resume_next([](const std::exception_ptr&) { return rpp::source::empty<int>(); })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from observable")
            {
                CHECK(mock.get_received_values() == std::vector{1, 2, 3});
                CHECK(mock.get_total_on_next_count() == 3);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }

    SUBCASE("observable with one error emission")
    {
        auto obs = rpp::source::create<int>([](const auto& sub) {
            sub.on_next(1);
            sub.on_next(2);
            sub.on_error(std::make_exception_ptr(std::runtime_error{""}));
        });
        SUBCASE("subscribe")
        {
            obs | rpp::operators::on_error_resume_next([](const std::exception_ptr&) {
                return rpp::source::just<TestType>(rpp::schedulers::immediate{}, 3);
            })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from both outer and inner observable")
            {
                CHECK(mock.get_received_values() == std::vector{1, 2, 3});
                CHECK(mock.get_total_on_next_count() == 3);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }

    SUBCASE("observable with two error emissions")
    {
        auto obs = rpp::source::create<int>([](const auto& sub) {
            sub.on_next(1);
            sub.on_next(2);
            sub.on_error(std::make_exception_ptr(std::runtime_error{""}));
            sub.on_error(std::make_exception_ptr(std::runtime_error{""}));
        });
        SUBCASE("subscribe")
        {
            obs | rpp::operators::on_error_resume_next([](const std::exception_ptr&) {
                return rpp::source::just<TestType>(rpp::schedulers::immediate{}, 3);
            })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer only receives values from first inner observable")
            {
                CHECK(mock.get_received_values() == std::vector{1, 2, 3});
                CHECK(mock.get_total_on_next_count() == 3);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }

    SUBCASE("inner observable different emission type")
    {
        auto obs = rpp::source::create<int>([](const auto& sub) {
            sub.on_next(1);
            sub.on_next(2);
            sub.on_error(std::make_exception_ptr(std::runtime_error{""}));
            sub.on_error(std::make_exception_ptr(std::runtime_error{""}));
        });
        SUBCASE("subscribe")
        {
            obs | rpp::operators::on_error_resume_next([](const std::exception_ptr&) {
                return rpp::source::just<TestType>(rpp::schedulers::immediate{}, 3);
            })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer only receives values from first inner observable")
            {
                CHECK(mock.get_received_values() == std::vector{1, 2, 3});
                CHECK(mock.get_total_on_next_count() == 3);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }

    SUBCASE("nested on_error_resume_next operators")
    {
        auto obs = rpp::source::create<int>([](const auto& sub) {
            sub.on_next(1);
            sub.on_error(std::make_exception_ptr(std::runtime_error{""}));
        });
        SUBCASE("subscribe")
        {
            obs | rpp::operators::on_error_resume_next([](const std::exception_ptr&) {
                return rpp::source::create<int>([](const auto& sub) {
                           sub.on_next(2);
                           sub.on_error(std::make_exception_ptr(std::runtime_error{""}));
                       })
                     | rpp::operators::on_error_resume_next([](const std::exception_ptr&) {
                           return rpp::source::create<int>([](const auto& sub) {
                               sub.on_next(3);
                               sub.on_completed();
                           });
                       });
            })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer receives values without any errors")
            {
                CHECK(mock.get_received_values() == std::vector{1, 2, 3});
                CHECK(mock.get_total_on_next_count() == 3);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }

    SUBCASE("selector throwing exception")
    {
        auto obs = rpp::source::create<int>([](const auto& sub) {
            sub.on_error(std::make_exception_ptr(std::runtime_error{""}));
        });
        SUBCASE("subscribe")
        {
            obs | rpp::operators::on_error_resume_next([](const std::exception_ptr& ep) {
                std::rethrow_exception(ep);
                return rpp::source::empty<int>();
            })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains selector error exception")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 1);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
    }
}

TEST_CASE("on_error_resume_next satisfies disposable contracts")
{
    auto observable_disposable = rpp::composite_disposable_wrapper::make();
    {
        auto observable = observable_with_disposable<int>(observable_disposable);
        auto op         = rpp::ops::on_error_resume_next([](const std::exception_ptr&) { return rpp::source::empty<int>(); });

        test_operator_with_disposable<int>(op);
        test_operator_finish_before_dispose<int>(op);
    }

    CHECK((observable_disposable.is_disposed() || observable_disposable.lock().use_count() == 2));
}
