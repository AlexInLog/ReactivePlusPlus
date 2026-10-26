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

#include <rpp/observables/dynamic_observable.hpp>
#include <rpp/observers/mock_observer.hpp>
#include <rpp/operators/flat_map.hpp>
#include <rpp/schedulers/immediate.hpp>
#include <rpp/sources/create.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/just.hpp>
#include <rpp/sources/never.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"

#include <stdexcept>
#include <string>

TEST_CASE_TEMPLATE("flat_map", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    auto mock = mock_observer_strategy<int>();
    SUBCASE("observable of items")
    {
        auto obs = rpp::source::just<TestType>(rpp::schedulers::immediate{}, 1, 2, 3);

        SUBCASE("subscribe using flat_map with templated lambda")
        {
            obs | rpp::operators::flat_map([](auto v) { return rpp::source::just(v * 2); })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from underlying observables")
            {
                CHECK(mock.get_received_values() == std::vector{2, 4, 6});
                CHECK(mock.get_on_completed_count() == 1);
                CHECK(mock.get_on_error_count() == 0);
            }
        }
        SUBCASE("subscribe using flat_map with templated lambda and pass operator by variable")
        {
            const auto multiply_by_two = rpp::operators::flat_map([](auto v) { return rpp::source::just(v * 2); });
            obs | multiply_by_two
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from underlying observables")
            {
                CHECK(mock.get_received_values() == std::vector{2, 4, 6});
                CHECK(mock.get_on_completed_count() == 1);
                CHECK(mock.get_on_error_count() == 0);
            }
        }
        SUBCASE("subscribe using flat_map with error")
        {
            obs | rpp::operators::flat_map([](int) { return rpp::source::error<int>(std::make_exception_ptr(std::runtime_error{""})); })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from underlying observables")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
                CHECK(mock.get_on_error_count() == 1);
            }
        }
        SUBCASE("subscribe using flat_map with empty")
        {
            obs | rpp::operators::flat_map([](int) { return rpp::source::empty<int>(); })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from underlying observables")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
                CHECK(mock.get_on_error_count() == 0);
            }
        }
        SUBCASE("subscribe using flat_map with never")
        {
            obs | rpp::operators::flat_map([](int) { return rpp::source::never<int>(); })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from underlying observables")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
                CHECK(mock.get_on_error_count() == 0);
            }
        }
        SUBCASE("subscribe using flat_map with empty in middle")
        {
            obs | rpp::operators::flat_map([](int v) {
                if (v == 2)
                    return rpp::source::empty<int>().as_dynamic();
                return rpp::source::just(v).as_dynamic();
            })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from underlying observables")
            {
                CHECK(mock.get_received_values() == std::vector{1, 3});
                CHECK(mock.get_on_completed_count() == 1);
                CHECK(mock.get_on_error_count() == 0);
            }
        }
        SUBCASE("subscribe using flat_map with never in middle")
        {
            obs | rpp::operators::flat_map([](int v) {
                if (v == 2)
                    return rpp::source::never<int>().as_dynamic();
                return rpp::source::just(v).as_dynamic();
            })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from underlying observables")
            {
                CHECK(mock.get_received_values() == std::vector{1, 3});
                CHECK(mock.get_on_completed_count() == 0);
                CHECK(mock.get_on_error_count() == 0);
            }
        }
        SUBCASE("subscribe using flat_map with error in middle")
        {
            obs | rpp::operators::flat_map([](int v) {
                if (v == 2)
                    return rpp::source::error<int>(std::make_exception_ptr(std::runtime_error{""})).as_dynamic();
                return rpp::source::just(v).as_dynamic();
            })
                | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from underlying observables")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
                CHECK(mock.get_on_error_count() == 1);
            }
        }
    }
}

TEST_CASE("flat_map satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::flat_map([](const auto& v) { return rpp::source::just(v); }));
}
