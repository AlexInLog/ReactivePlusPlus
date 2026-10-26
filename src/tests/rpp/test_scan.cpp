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
#include <rpp/operators/scan.hpp>
#include <rpp/sources/just.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"


TEST_CASE_TEMPLATE("scan scans values and store state", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    auto obs = rpp::source::just<TestType>(1, 2, 3);

    SUBCASE("subscribe on it via scan with plus")
    {
        auto mock = mock_observer_strategy<int>{};

        obs | rpp::operators::scan(10, std::plus<int>{}) | rpp::operators::subscribe(mock);
        SUBCASE("observer obtains partial sums")
        {
            CHECK(mock.get_received_values() == std::vector{10, 11, 13, 16});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 1);
        }
    }
    SUBCASE("subscribe on it via scan with plus with no seed")
    {
        auto mock = mock_observer_strategy<int>{};

        obs | rpp::operators::scan(std::plus<int>{}) | rpp::operators::subscribe(mock);
        SUBCASE("observer obtains partial sums")
        {
            CHECK(mock.get_received_values() == std::vector{1, 3, 6});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 1);
        }
    }
    SUBCASE("subscribe on it via scan as lvalue with plus")
    {
        auto mock = mock_observer_strategy<int>{};

        auto op = rpp::operators::scan(10, std::plus<int>{});
        obs | op | rpp::operators::subscribe(mock);
        SUBCASE("observer obtains partial sums")
        {
            CHECK(mock.get_received_values() == std::vector{10, 11, 13, 16});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 1);
        }
    }
    SUBCASE("subscribe on it via scan with plus as lvalue with no seed")
    {
        auto mock = mock_observer_strategy<int>{};

        auto op = rpp::operators::scan(std::plus<int>{});
        obs | op | rpp::operators::subscribe(mock);
        SUBCASE("observer obtains partial sums")
        {
            CHECK(mock.get_received_values() == std::vector{1, 3, 6});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 1);
        }
    }
    SUBCASE("subscribe on it via scan with aggregating in vector")
    {
        auto mock = mock_observer_strategy<std::vector<int>>{};

        obs
            | rpp::operators::scan(std::vector<int>{},
                                   [](std::vector<int>&& seed, int new_val) {
                                       seed.push_back(new_val);
                                       return std::move(seed);
                                   })
            | rpp::operators::subscribe(mock);

        SUBCASE("observer obtains partial vectors")
        {
            CHECK(mock.get_received_values() == std::vector{std::vector<int>{}, std::vector{1}, std::vector{1, 2}, std::vector{1, 2, 3}});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 1);
        }
    }
    SUBCASE("subscribe on it via scan with exception")
    {
        auto mock = mock_observer_strategy<int>{};

        volatile bool none{};
        obs | rpp::operators::scan(0, [&](int, int) -> int {
            if (none)
                return 0;
            throw std::runtime_error{""};
        }) | rpp::operators::subscribe(mock);

        SUBCASE("observer obtains only on_error")
        {
            CHECK(mock.get_received_values() == std::vector{0});
            CHECK(mock.get_on_error_count() == 1);
            CHECK(mock.get_on_completed_count() == 0);
        }
    }
    SUBCASE("subscribe on it via scan with exception with no seed")
    {
        auto mock = mock_observer_strategy<int>{};

        volatile bool none{};
        obs | rpp::operators::scan([&](int, int) -> int {
            if (none)
                return 0;
            throw std::runtime_error{""};
        })
            | rpp::operators::subscribe(mock);

        SUBCASE("observer obtains only on_error")
        {
            CHECK(mock.get_received_values() == std::vector{1});
            CHECK(mock.get_on_error_count() == 1);
            CHECK(mock.get_on_completed_count() == 0);
        }
    }
}

TEST_CASE("scan doesn't produce extra copies")
{
    SUBCASE("scan([](verifier&& seed, auto&& v){return forward(v); }")
    {
        SUBCASE("send value by copy")
        {
            copy_count_tracker tracker{};
            tracker.get_observable(2) | rpp::ops::scan([](copy_count_tracker&&, auto&& value) { return std::forward<decltype(value)>(value); }) | rpp::ops::subscribe([](copy_count_tracker) { }); // NOLINT

            // first emission: 1 copy to state + 1 copy to subscriber
            // second emision: 1 copy FROM scan lambda + 1 move to internal state + 1 copy to subscriber

            CHECK(tracker.get_copy_count() == 4);
            CHECK(tracker.get_move_count() == 1);
        }

        SUBCASE("send value by move")
        {
            copy_count_tracker tracker{};
            tracker.get_observable_for_move(2) | rpp::ops::scan([](copy_count_tracker&&, auto&& value) { return std::forward<decltype(value)>(value); }) | rpp::ops::subscribe([](copy_count_tracker) { }); // NOLINT

            // first emission: 1 move to state + 1 copy to subscriber
            // second emision: 1 move FROM scan lambda + 1 move to internal state + 1 copy to subscriber

            CHECK(tracker.get_copy_count() == 2); // 2 times 1 copy to final subcriber
            CHECK(tracker.get_move_count() == 3); // 2 times 1 move to internal state
        }
    }
}

TEST_CASE("scan satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::scan([](auto&& s, auto&&) { return s; }));
}
