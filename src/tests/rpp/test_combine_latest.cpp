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

#include <rpp/operators/as_blocking.hpp>
#include <rpp/operators/combine_latest.hpp>
#include <rpp/schedulers/current_thread.hpp>
#include <rpp/schedulers/immediate.hpp>
#include <rpp/sources/concat.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/just.hpp>
#include <rpp/sources/never.hpp>
#include <rpp/subjects/publish_subject.hpp>

#include "disposable_observable.hpp"
#include "rpp_trompeloil.hpp"

TEST_CASE("combine_latest bundles items")
{
    SUBCASE("observable of -1-2-3-| combines with -4-5-6-| on immediate scheduler")
    {
        auto                  mock = mock_observer<std::tuple<int, int>>{};
        trompeloeil::sequence seq;

        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(1, 6))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(2, 6))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(3, 6))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(seq);

        rpp::source::just(rpp::schedulers::immediate{}, 1, 2, 3)
            | rpp::ops::combine_latest(rpp::source::just(rpp::schedulers::immediate{}, 4, 5, 6))
            | rpp::ops::subscribe(mock);
    }

    SUBCASE("observable of -1-2-3-| combines with -4-5-6-| on current_thread")
    {
        auto                  mock = mock_observer<std::tuple<int, int>>{};
        trompeloeil::sequence seq;

        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(1, 4))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(1, 5))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(2, 5))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(2, 6))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(3, 6))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(seq);

        rpp::source::just(rpp::schedulers::current_thread{}, 1, 2, 3)                                 // source 1
            | rpp::ops::combine_latest(rpp::source::just(rpp::schedulers::current_thread{}, 4, 5, 6)) // source 2
            | rpp::ops::subscribe(mock);

        // Above stream should output in such sequence
        // source 1:   -1---2---3-|
        // source 2: -4---5---6-|
    }

    SUBCASE("observable of -1-2-3-| combines with two other sources on current_thread")
    {
        auto                  mock = mock_observer<std::tuple<int, int, int>>{};
        trompeloeil::sequence seq;

        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(1, 4, 7))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(1, 5, 7))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(1, 5, 8))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(2, 5, 8))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(2, 6, 8))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(2, 6, 9))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue(std::make_tuple(3, 6, 9))).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(seq);

        rpp::source::just(rpp::schedulers::current_thread{}, 1, 2, 3) // source 1
            | rpp::ops::combine_latest(
                rpp::source::just(rpp::schedulers::current_thread{}, 4, 5, 6), // source 2
                rpp::source::just(rpp::schedulers::current_thread{}, 7, 8, 9)) // source 3
            | rpp::ops::subscribe(mock);

        // Above stream should output in such sequence
        // source 1:   --1---2---3-|
        // source 2: -4---5---6-|
        // source 3: --7---8---9-|
    }
}

TEST_CASE("combine_latest defers completed event")
{
    SUBCASE("observable of -1-2-3-| and never")
    {
        auto mock = mock_observer<std::tuple<int, int>>{};

        rpp::source::just(1, 2, 3)
            | rpp::ops::combine_latest(rpp::source::never<int>())
            | rpp::ops::subscribe(mock);
    }
}

TEST_CASE("combine_latest forwards errors")
{
    SUBCASE("observable of -1-2-3-| combines with error")
    {
        auto mock = mock_observer<std::tuple<int, int>>{};
        REQUIRE_CALL(*mock, on_error(trompeloeil::_));
        rpp::source::just(1, 2, 3)
            | rpp::ops::combine_latest(rpp::source::error<int>(std::make_exception_ptr(std::runtime_error{""})))
            | rpp::ops::subscribe(mock);
    }
}

TEST_CASE("combine_latest handles race condition")
{
    SUBCASE("suscribe on source observable in current thread pairs with error in other thread - on_error can't interleave with on_next")
    {
        auto                  subject = rpp::subjects::publish_subject<int>{};
        auto                  mock    = mock_observer<std::tuple<int, int>>{};
        trompeloeil::sequence s{};

        REQUIRE_CALL(*mock, on_next_rvalue(trompeloeil::_))
            .TIMES(AT_LEAST(1))
            .IN_SEQUENCE(s)
            .LR_SIDE_EFFECT({
                std::thread{[subject] {
                    subject.get_observer().on_error(std::exception_ptr{});
                }}.detach();
                std::this_thread::sleep_for(std::chrono::seconds{1});
            });
        REQUIRE_CALL(*mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);

        rpp::source::just(1, 1, 1)
            | rpp::ops::combine_latest(rpp::source::concat(rpp::source::just(2), subject.get_observable()))
            | rpp::ops::as_blocking()
            | rpp::ops::subscribe(mock);
    }
}

TEST_CASE("combine_latest is not deadlocking is_disposed")
{
    std::optional<rpp::dynamic_observer<int>> observer{};

    rpp::source::create<int>([&observer](auto&& obs) {
        observer = std::forward<decltype(obs)>(obs).as_dynamic();
        observer->on_next(1);
    })
        | rpp::operators::combine_latest(rpp::source::just(1))
        | rpp::ops::subscribe([&observer](auto) {
              CHECK(observer);
              CHECK(!observer->is_disposed());
          });
}

TEST_CASE("combine_latest satisfies disposable contracts")
{
    auto observable_disposable = rpp::composite_disposable_wrapper::make();
    {
        auto observable = observable_with_disposable<int>(observable_disposable);
        auto op         = rpp::ops::combine_latest(observable);

        test_operator_with_disposable<int>(op);
        test_operator_finish_before_dispose<int>(op);
    }

    CHECK((observable_disposable.is_disposed() || observable_disposable.lock().use_count() == 2));
}
