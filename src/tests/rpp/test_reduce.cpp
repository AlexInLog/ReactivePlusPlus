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
#include <rpp/operators/reduce.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/from.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"

TEST_CASE_TEMPLATE("reduce reduces values and store state", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    auto obs = rpp::source::just<TestType>(1, 2, 3);

    SUBCASE("subscribe on it with plus and initial seed")
    {
        auto mock = mock_observer_strategy<int>{};

        obs | rpp::operators::reduce(0, std::plus<int>{}) | rpp::operators::subscribe(mock);

        SUBCASE("observer obtains sum")
        {
            CHECK(mock.get_received_values() == std::vector{6});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 1);
        }
    }

    SUBCASE("subscribe on it with plus and no initial seed")
    {
        auto mock = mock_observer_strategy<int>{};

        obs | rpp::operators::reduce(std::plus<int>{}) | rpp::operators::subscribe(mock);

        SUBCASE("observer obtains sum")
        {
            CHECK(mock.get_received_values() == std::vector{6});
            CHECK(mock.get_on_error_count() == 0);
            CHECK(mock.get_on_completed_count() == 1);
        }
    }

    SUBCASE("subscribe on it with exception and no seed")
    {
        auto mock = mock_observer_strategy<int>{};

        obs | rpp::operators::reduce([](int seed, int value) {
            if (seed == 1)
                return value;
            throw std::runtime_error{""};
        }) | rpp::operators::subscribe(mock);

        SUBCASE("observer obtains only on_error")
        {
            CHECK(mock.get_total_on_next_count() == 0);
            CHECK(mock.get_on_error_count() == 1);
            CHECK(mock.get_on_completed_count() == 0);
        }
    }
}

TEST_CASE("reduce forwards callbacks")
{
    auto mock = mock_observer_strategy<int>{};
    SUBCASE("on_error")
    {
        rpp::source::error<int>({})
            | rpp::ops::reduce(0, std::plus<int>{})
            | rpp::ops::subscribe(mock);

        CHECK(mock.get_received_values().empty());
        CHECK(mock.get_on_error_count() == 1);
    }

    SUBCASE("on_completed")
    {
        rpp::source::empty<int>()
            | rpp::ops::reduce(0, std::plus<int>{})
            | rpp::ops::subscribe(mock);

        CHECK(mock.get_received_values() == std::vector<int>{0});
        CHECK(mock.get_on_completed_count() == 1);
    }

    SUBCASE("on_completed no_seed")
    {
        rpp::source::empty<int>()
            | rpp::ops::reduce(std::plus<int>{})
            | rpp::ops::subscribe(mock);

        CHECK(mock.get_received_values().empty());
        CHECK(mock.get_on_completed_count() == 1);
    }
}

TEST_CASE("reduce doesn't produce extra copies")
{
    SUBCASE("reduce([](verifier&& seed, auto&& v){return forward(v); }")
    {
        SUBCASE("send value by copy")
        {
            copy_count_tracker tracker{};
            tracker.get_observable(2) | rpp::ops::reduce([](copy_count_tracker&&, auto&& value) { return std::forward<decltype(value)>(value); }) | rpp::ops::subscribe([](copy_count_tracker) { }); // NOLINT

            // first emission: 1 copy to seed
            // second emision: 1 copy FROM lambda + 1 move to seed + 1 move to subscriber

            CHECK(tracker.get_copy_count() == 2);
            CHECK(tracker.get_move_count() == 2);
        }

        SUBCASE("send value by move")
        {
            copy_count_tracker tracker{};
            tracker.get_observable_for_move(2) | rpp::ops::reduce([](copy_count_tracker&&, auto&& value) { return std::forward<decltype(value)>(value); }) | rpp::ops::subscribe([](copy_count_tracker) { }); // NOLINT

            // first emission: 1 move to seed
            // second emision: 1 move FROM lambda + 1 move to seed + 1 move to subscriber

            CHECK(tracker.get_copy_count() == 0);
            CHECK(tracker.get_move_count() == 4);
        }
    }
}

TEST_CASE("reduce satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::reduce([](auto&& s, auto&&) { return s; }));
}
