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
#include <rpp/operators/as_blocking.hpp>
#include <rpp/operators/repeat_when.hpp>
#include <rpp/schedulers/immediate.hpp>
#include <rpp/schedulers/new_thread.hpp>
#include <rpp/sources/concat.hpp>
#include <rpp/sources/create.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/just.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"
#include "rpp_trompeloil.hpp"

#include <string>

TEST_CASE("repeat_when resubscribes on notifier emission")
{
    mock_observer<std::string> mock{};
    trompeloeil::sequence      seq;
    size_t                     subscribe_count = 0;
    auto                       observable      = rpp::source::create<std::string>([&subscribe_count](const auto& sub) {
        sub.on_next(std::to_string(++subscribe_count));
        sub.on_completed();
    });

    SUBCASE("empty notifier")
    {
        REQUIRE_CALL(*mock, on_next_rvalue("1")).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(seq);

        observable
            | rpp::operators::repeat_when([]() { return rpp::source::empty<int>(); })
            | rpp::operators::subscribe(mock);

        CHECK(subscribe_count == 1);
    }

    SUBCASE("notifier resubscribes once")
    {
        size_t i = 0;

        REQUIRE_CALL(*mock, on_next_rvalue("1")).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue("2")).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(seq);

        observable
            | rpp::operators::repeat_when([&i]() {
                  if (i++ != 0)
                      return rpp::source::empty<int>().as_dynamic();
                  return rpp::source::just(1).as_dynamic();
              })
            | rpp::operators::subscribe(mock);

        CHECK(subscribe_count == 2);
    }

    SUBCASE("notifier resubscribes multiple times")
    {
        size_t i = 0;

        REQUIRE_CALL(*mock, on_next_rvalue("1")).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue("2")).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue("3")).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_next_rvalue("4")).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(seq);

        observable
            | rpp::operators::repeat_when([&i]() {
                  if (i++ >= 3)
                      return rpp::source::empty<int>().as_dynamic();
                  return rpp::source::just(1).as_dynamic();
              })
            | rpp::operators::subscribe(mock);

        CHECK(subscribe_count == 4);
    }

    SUBCASE("notifier throws")
    {
        REQUIRE_CALL(*mock, on_next_rvalue("1")).IN_SEQUENCE(seq);
        REQUIRE_CALL(*mock, on_error(trompeloeil::_)).IN_SEQUENCE(seq);

        observable
            | rpp::operators::repeat_when([]() {
                  throw std::runtime_error{""};
                  return rpp::source::just(1);
              })
            | rpp::operators::subscribe(mock);

        CHECK(subscribe_count == 1);
    }
}

TEST_CASE("repeat_when does not stack overflow")
{
    mock_observer<int>    mock{};
    trompeloeil::sequence seq;

    constexpr size_t count = 500000;

    REQUIRE_CALL(*mock, on_next_rvalue(trompeloeil::_)).TIMES(count).IN_SEQUENCE(seq);
    REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(seq);

    rpp::source::create<int>([](const auto& obs) {
        obs.on_next(1);
        obs.on_completed();
    })
        | rpp::operators::repeat_when([i = count]() mutable {
            if (--i != 0)
                return rpp::source::just(rpp::schedulers::immediate{}, 1).as_dynamic(); // Use immediate scheduler for recursion
            return rpp::source::empty<int>().as_dynamic(); })
        | rpp::operators::subscribe(mock);
}

TEST_CASE("repeat_when disposes on looping")
{
    mock_observer<int> mock{};
    REQUIRE_CALL(*mock, on_next_rvalue(1)).TIMES(2);
    REQUIRE_CALL(*mock, on_completed());

    size_t i = 0;

    rpp::source::concat(rpp::source::create<int>([](auto&& subscriber) {
        auto d = rpp::composite_disposable_wrapper::make();
        subscriber.set_upstream(d);
        subscriber.on_next(1);
        subscriber.on_completed();
        CHECK(d.is_disposed());
    })) | rpp::ops::repeat_when([&i]() { return i++ ? rpp::source::empty<int>().as_dynamic() : rpp::source::just(1).as_dynamic(); })
        | rpp::ops::subscribe(mock);
}

TEST_CASE("repeat_when doesn't produce extra copies")
{
    SUBCASE("repeat_when(empty_notifier)")
    {
        copy_count_tracker::test_operator(rpp::ops::repeat_when([]() { return rpp::source::empty<int>(); }),
                                          {
                                              .send_by_copy = {.copy_count = 1, // 1 copy to final subscriber
                                                               .move_count = 0},
                                              .send_by_move = {.copy_count = 0,
                                                               .move_count = 1} // 1 move to final subscriber
                                          });
    }
}

TEST_CASE("repeat_when satisfies disposable contracts")
{

    test_operator_with_disposable<int>(rpp::ops::repeat_when([]() { return rpp::source::empty<int>(); }));
    test_operator_finish_before_dispose<int>(rpp::ops::repeat_when([]() { return rpp::source::empty<int>(); }));

    test_operator_over_observable_with_disposable<int>(
        [](auto observable) {
            auto c = std::make_shared<size_t>();
            return rpp::source::concat(observable, rpp::source::error<int>(std::make_exception_ptr(std::runtime_error{"error"})))
                 | rpp::ops::repeat_when([c]() -> rpp::dynamic_observable<int> {  if ((*c)++ ==0) return rpp::source::just(1); return rpp::source::empty<int>(); });
        });
}
