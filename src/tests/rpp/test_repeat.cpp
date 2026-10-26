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
#include <rpp/operators/repeat.hpp>
#include <rpp/operators/take.hpp>
#include <rpp/sources/create.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"
#include "rpp_trompeloil.hpp"


TEST_CASE("repeat resubscribes")
{
    auto observer = mock_observer_strategy<int>();
    SUBCASE("observable with value")
    {
        size_t subscribe_count = 0;
        auto   observable      = rpp::source::create<int>([&subscribe_count](const auto& sub) {
            ++subscribe_count;
            sub.on_next(1);
            sub.on_completed();
        });

        SUBCASE("subscribe on it via repeat(0)")
        {
            observable | rpp::operators::repeat(0) | rpp::operators::subscribe(observer);
            SUBCASE("only on_completed sent")
            {
                CHECK(subscribe_count == 0);
                CHECK(observer.get_total_on_next_count() == 0);
                CHECK(observer.get_on_error_count() == 0);
                CHECK(observer.get_on_completed_count() == 1);
            }
        }
        SUBCASE("subscribe on it via repeat(1)")
        {
            observable | rpp::operators::repeat(1) | rpp::operators::subscribe(observer);
            SUBCASE("sent value once")
            {
                CHECK(subscribe_count == 1);
                CHECK(observer.get_total_on_next_count() == 1);
                CHECK(observer.get_on_error_count() == 0);
                CHECK(observer.get_on_completed_count() == 1);
            }
        }
        SUBCASE("subscribe on it via repeat(10)")
        {
            observable | rpp::operators::repeat(10) | rpp::operators::subscribe(observer);
            SUBCASE("sent value 10 times")
            {
                CHECK(subscribe_count == 10);
                CHECK(observer.get_total_on_next_count() == 10);
                CHECK(observer.get_on_error_count() == 0);
                CHECK(observer.get_on_completed_count() == 1);
            }
        }
        SUBCASE("subscribe on it via repeat()")
        {
            observable | rpp::operators::repeat() | rpp::operators::take(10) | rpp::operators::subscribe(observer);
            SUBCASE("sent value infinitely")
            {
                CHECK(subscribe_count == 10);
                CHECK(observer.get_total_on_next_count() == 10);
                CHECK(observer.get_on_error_count() == 0);
                CHECK(observer.get_on_completed_count() == 1);
            }
        }
    }
    SUBCASE("observable with on_error")
    {
        size_t subscribe_count = 0;
        auto   observable      = rpp::source::create<int>([&subscribe_count](const auto& sub) {
            ++subscribe_count;
            sub.on_error(std::make_exception_ptr(std::runtime_error{""}));
        });
        SUBCASE("subscribe on it via repeat(10)")
        {
            observable | rpp::operators::repeat(10) | rpp::operators::subscribe(observer);
            SUBCASE("only on_error once")
            {
                CHECK(subscribe_count == 1);
                CHECK(observer.get_total_on_next_count() == 0);
                CHECK(observer.get_on_error_count() == 1);
                CHECK(observer.get_on_completed_count() == 0);
            }
        }
    }
    SUBCASE("observable with on_completed")
    {
        size_t subscribe_count = 0;
        auto   observable      = rpp::source::create<int>([&subscribe_count](const auto& sub) {
            ++subscribe_count;
            sub.on_completed();
        });
        SUBCASE("subscribe on it via repeat(10)")
        {
            observable | rpp::operators::repeat(10) | rpp::operators::subscribe(observer);
            SUBCASE("on_ompleted once")
            {
                CHECK(subscribe_count == 10);
                CHECK(observer.get_total_on_next_count() == 0);
                CHECK(observer.get_on_error_count() == 0);
                CHECK(observer.get_on_completed_count() == 1);
            }
        }
    }
}

TEST_CASE("repeat doesn't produce extra copies")
{
    SUBCASE("repeat(2)")
    {
        copy_count_tracker::test_operator(rpp::ops::repeat(2),
                                          {
                                              .send_by_copy = {.copy_count = 2, // 2 times 1 copy to final subscriber
                                                               .move_count = 0},
                                              .send_by_move = {.copy_count = 0,
                                                               .move_count = 2} // 2 times 1 move to final subscriber
                                          });
    }
}

TEST_CASE("repeat satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::repeat(1));
}


TEST_CASE("repeat handles stack overflow")
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
        | rpp::operators::repeat(count)
        | rpp::operators::subscribe(mock);
}
