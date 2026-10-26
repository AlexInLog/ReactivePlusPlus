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

#include <rpp/operators/map.hpp>
#include <rpp/sources/just.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"
#include "rpp_trompeloil.hpp"

#include <stdexcept>
#include <string>

TEST_CASE_TEMPLATE("map modifies values and forward errors/completions", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    auto obs = rpp::source::just<TestType>(1, 2);

    SUBCASE("map changes value")
    {
        mock_observer<std::string> mock{};
        trompeloeil::sequence      seq;

        REQUIRE_CALL(*mock, on_next_rvalue("TEST 1")).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue("TEST 2")).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(seq);

        obs | rpp::operators::map([](auto v) { return std::string("TEST ") + std::to_string(v); }) | rpp::operators::subscribe(std::move(mock));
    }


    SUBCASE("map with exception value")
    {
        mock_observer<int>    mock{};
        trompeloeil::sequence seq;

        REQUIRE_CALL(*mock, on_error(trompeloeil::_)).IN_SEQUENCE(seq);

        auto map = rpp::operators::map([](int) -> int { throw std::runtime_error{"map failed"}; });

        obs | map | rpp::operators::subscribe(std::move(mock)); // NOLINT
    }
}


TEST_CASE("map doesn't produce extra copies")
{
    SUBCASE("map([](auto&& v){return std::forward(v);})")
    {
        copy_count_tracker::test_operator(rpp::ops::map([](auto&& v) { return std::forward<decltype(v)>(v); }),
                                          {
                                              .send_by_copy = {.copy_count = 1,  // 1 copy on return from map
                                                               .move_count = 1}, // 1 move to final subscriber
                                              .send_by_move = {.copy_count = 0,
                                                               .move_count = 2} // 1 move on return from map + 1 move to final subscriber
                                          });
    }
}

TEST_CASE("map satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::map([](auto&& v) { return std::forward<decltype(v)>(v); }));
}
