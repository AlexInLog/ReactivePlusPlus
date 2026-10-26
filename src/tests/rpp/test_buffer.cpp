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
#include <rpp/operators/buffer.hpp>
#include <rpp/operators/merge.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/just.hpp>

#include "disposable_observable.hpp"
#include "rpp_trompeloil.hpp"

TEST_CASE("buffer bundles items")
{
    trompeloeil::sequence s{};
    auto                  mock = mock_observer<std::vector<int>>{};

    SUBCASE("observable of -1-2-3-|")
    {
        auto obs = rpp::source::just(1, 2, 3);
        SUBCASE("buffer(0) - shall see -{1}-{2}-{3}-|")
        {
            REQUIRE_CALL(*mock, on_next_rvalue(std::vector{1})).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_rvalue(std::vector{2})).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_rvalue(std::vector{3})).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);

            obs | rpp::ops::buffer(0) | rpp::ops::subscribe(mock);
        }
        SUBCASE("buffer(1) - shall see -{1}-{2}-{3}-|")
        {
            REQUIRE_CALL(*mock, on_next_rvalue(std::vector{1})).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_rvalue(std::vector{2})).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_rvalue(std::vector{3})).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);

            obs | rpp::ops::buffer(1)
                | rpp::ops::subscribe(mock);
        }
        SUBCASE("buffer(2) - shall see -{1,2}-{3}|")
        {
            REQUIRE_CALL(*mock, on_next_rvalue(std::vector{1, 2})).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_rvalue(std::vector{3})).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);

            obs | rpp::ops::buffer(2)
                | rpp::ops::subscribe(mock);
        }
        SUBCASE("buffer(3) - shall see -{1,2,3}-|")
        {
            REQUIRE_CALL(*mock, on_next_rvalue(std::vector{1, 2, 3})).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);

            obs | rpp::ops::buffer(3)
                | rpp::ops::subscribe(mock);
        }
        SUBCASE("buffer(4) - shall see -{1,2,3}-|")
        {
            REQUIRE_CALL(*mock, on_next_rvalue(std::vector{1, 2, 3})).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);

            obs | rpp::ops::buffer(4)
                | rpp::ops::subscribe(mock);
        }
    }

    SUBCASE("observable of -1-x-2-|, which error is raised in the middle")
    {
        auto obs = rpp::source::just(rpp::source::just(1).as_dynamic(),
                                     rpp::source::error<int>(std::make_exception_ptr(std::runtime_error{""})).as_dynamic(),
                                     rpp::source::just(2).as_dynamic())
                 | rpp::ops::merge();
        SUBCASE("buffer(0) - shall see -{1}-x, which means error event is through")
        {
            REQUIRE_CALL(*mock, on_next_rvalue(std::vector{1})).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);

            obs | rpp::ops::buffer(0)
                | rpp::ops::subscribe(mock);
        }
        SUBCASE("buffer(1) - shall see -{1}-x, which means error event is through")
        {
            REQUIRE_CALL(*mock, on_next_rvalue(std::vector{1})).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);

            obs | rpp::ops::buffer(1)
                | rpp::ops::subscribe(mock);
        }
        SUBCASE("buffer(2) - shall see --x, which means error event is through")
        {
            REQUIRE_CALL(*mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);

            obs | rpp::ops::buffer(2)
                | rpp::ops::subscribe(mock);
        }
    }
    SUBCASE("accept by moving")
    {
        REQUIRE_CALL(*mock, on_next_rvalue(std::vector{1, 2})).IN_SEQUENCE(s);
        REQUIRE_CALL(*mock, on_next_rvalue(std::vector{3, 4})).IN_SEQUENCE(s);

        rpp::source::just(1, 2, 3, 4)
            | rpp::ops::buffer(2)
            | rpp::ops::subscribe([&mock](std::vector<int> b) // NOLINT
                                  {
                                      mock.on_next(std::move(b));
                                  });
    }
}

TEST_CASE("buffer satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::buffer(1));
}
