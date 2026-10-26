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
#include <rpp/operators/finally.hpp>
#include <rpp/sources/create.hpp>

#include "disposable_observable.hpp"

TEST_CASE("finally executes only at the end")
{
    auto mock = mock_observer_strategy<int>();
    SUBCASE("observable with no emissions")
    {
        auto   obs     = rpp::source::create<int>([](const auto&) {
        });
        size_t invoked = 0;
        SUBCASE("subscribe")
        {
            obs | rpp::operators::finally([&]() noexcept { ++invoked; })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from observable")
            {
                CHECK(invoked == 1);
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
    }

    SUBCASE("observable with on_completed emission")
    {
        auto   obs     = rpp::source::create<int>([](const auto& sub) {
            sub.on_completed();
        });
        size_t invoked = 0;
        SUBCASE("subscribe")
        {
            obs | rpp::operators::finally([&]() noexcept { ++invoked; })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from observable")
            {
                CHECK(invoked == 1);
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }

    SUBCASE("observable with on_next emission")
    {
        auto   obs     = rpp::source::create<int>([](const auto& sub) {
            sub.on_next(1);
            sub.on_completed();
        });
        size_t invoked = 0;
        SUBCASE("subscribe")
        {
            obs | rpp::operators::finally([&]() noexcept { ++invoked; })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from observable")
            {
                CHECK(invoked == 1);
                CHECK(mock.get_total_on_next_count() == 1);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }

    SUBCASE("observable with on_error emission")
    {
        auto   obs     = rpp::source::create<int>([](const auto& sub) {
            sub.on_next(1);
            sub.on_error(std::make_exception_ptr(std::runtime_error{""}));
        });
        size_t invoked = 0;
        SUBCASE("subscribe")
        {
            obs | rpp::operators::finally([&]() noexcept { ++invoked; })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from observable")
            {
                CHECK(invoked == 1);
                CHECK(mock.get_total_on_next_count() == 1);
                CHECK(mock.get_on_error_count() == 1);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
    }
}

TEST_CASE("finally satisfies disposable contracts")
{
    auto observable_disposable = rpp::composite_disposable_wrapper::make();
    {
        auto observable = observable_with_disposable<int>(observable_disposable);

        test_operator_with_disposable<int>(
            rpp::ops::finally([]() noexcept { }));
    }

    CHECK((observable_disposable.is_disposed() || observable_disposable.lock().use_count() == 2));
}
