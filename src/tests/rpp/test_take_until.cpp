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
#include <rpp/operators/as_blocking.hpp>
#include <rpp/operators/finally.hpp>
#include <rpp/operators/take.hpp>
#include <rpp/operators/take_until.hpp>
#include <rpp/schedulers/current_thread.hpp>
#include <rpp/sources/concat.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/interval.hpp>
#include <rpp/sources/just.hpp>
#include <rpp/sources/never.hpp>
#include <rpp/subjects/publish_subject.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"

TEST_CASE("take_until mirrors both source observable and trigger observable")
{
    auto mock = mock_observer_strategy<int>{};
    SUBCASE("observable of -1-2... pairs with trigger observable from a publish subject")
    {
        auto test = [&mock](auto inner_action) {
            auto subject          = rpp::subjects::publish_subject<bool>{};
            auto other_subscriber = subject.get_observer();

            rpp::source::create<int>([&other_subscriber, &inner_action](const auto& subscriber) {
                subscriber.on_next(1);
                subscriber.on_next(2);

                // Should see a terminate event after this
                inner_action(other_subscriber);

                subscriber.on_next(3);
            })
                | rpp::ops::take_until(subject.get_observable())
                | rpp::ops::subscribe(mock);
        };

        SUBCASE("subcject emits on_next")
        {
            test([](const auto& sub) { sub.on_next(true); });
            SUBCASE("should see -1-2-|")
            {
                CHECK(mock.get_received_values() == std::vector<int>{1, 2});
                CHECK(mock.get_on_completed_count() == 1);
                CHECK(mock.get_on_error_count() == 0);
            }
        }
        SUBCASE("subcject emits on_error")
        {
            test([](const auto& sub) { sub.on_error({}); });
            SUBCASE("should see -1-2-x")
            {
                CHECK(mock.get_received_values() == std::vector<int>{1, 2});
                CHECK(mock.get_on_completed_count() == 0);
                CHECK(mock.get_on_error_count() == 1);
            }
        }
        SUBCASE("subcject emits on_completed")
        {
            test([](const auto& sub) { sub.on_completed(); });
            SUBCASE("should see -1-2-|")
            {
                CHECK(mock.get_received_values() == std::vector<int>{1, 2});
                CHECK(mock.get_on_completed_count() == 1);
                CHECK(mock.get_on_error_count() == 0);
            }
        }
    }

    SUBCASE("observable of -1-2-3-| pairs with trigger observable of never")
    {
        rpp::source::just(1, 2, 3)
            | rpp::ops::take_until(rpp::source::never<bool>())
            | rpp::ops::subscribe(mock);

        SUBCASE("should see -1-2-3-|")
        {
            CHECK(mock.get_received_values() == std::vector<int>{1, 2, 3});
            CHECK(mock.get_on_completed_count() == 1);
            CHECK(mock.get_on_error_count() == 0);
        }
    }

    SUBCASE("observable of -1-| pairs with trigger observable of -1-|")
    {
        rpp::source::just(1)
            | rpp::ops::take_until(rpp::source::just(1))
            | rpp::ops::subscribe(mock);

        SUBCASE("should see -| because take_until is subscribed first and it also mirrors the trigger observable 's completed event")
        {
            CHECK(mock.get_received_values().empty());
            CHECK(mock.get_on_completed_count() == 1);
            CHECK(mock.get_on_error_count() == 0);
        }
    }

    SUBCASE("observable of -1-| pairs with trigger observable of -x")
    {
        rpp::source::just(1)
            | rpp::ops::take_until(rpp::source::error<bool>(std::make_exception_ptr(std::runtime_error{""})))
            | rpp::ops::subscribe(mock);

        SUBCASE("should see -x because take_until also mirrors the trigger observable 's error event")
        {
            CHECK(mock.get_received_values().empty());
            CHECK(mock.get_on_completed_count() == 0);
            CHECK(mock.get_on_error_count() == 1);
        }
    }

    SUBCASE("observable of -1-| pairs with trigger observable of -|")
    {
        rpp::source::just(1)
            | rpp::ops::take_until(rpp::source::empty<bool>())
            | rpp::ops::subscribe(mock);

        SUBCASE("should see -| because take_until completes prior to the source")
        {
            CHECK(mock.get_received_values().empty());
            CHECK(mock.get_on_completed_count() == 1);
            CHECK(mock.get_on_error_count() == 0);
        }
    }
}

TEST_CASE("take_until can handle race condition")
{
    SUBCASE("observer consumes on_next slower than source sends on_next and on_completed events")
    {
        std::atomic_bool on_completed_called{false};
        auto             subject = rpp::subjects::publish_subject<int>{};

        SUBCASE("on_completed shall not interleave with on_next")
        {
            rpp::source::concat(rpp::source::just(1) | rpp::ops::take(1), rpp::source::never<int>())
                | rpp::ops::take_until(subject.get_observable())
                | rpp::ops::as_blocking()
                | rpp::ops::subscribe([&](auto&&) {
                      CHECK(!on_completed_called);
                      std::thread{[&] {
                          subject.get_observer().on_completed();
                      }}.detach();
                      std::this_thread::sleep_for(std::chrono::milliseconds{400});
                      CHECK(!on_completed_called);
                  } /* on_next */,
                                      {} /* on_error */,
                                      [&]() {
                                          on_completed_called = true;
                                      } /* on_error */);

            CHECK(on_completed_called);
        }
    }

    SUBCASE("observer consumes on_next slower than source sends on_next and on_error events")
    {
        std::atomic_bool on_error_called{false};
        auto             subject = rpp::subjects::publish_subject<int>{};

        SUBCASE("on_error shall not interleave with on_next")
        {
            rpp::source::concat(rpp::source::just(1) | rpp::ops::take(1), rpp::source::never<int>())
                | rpp::ops::take_until(subject.get_observable())
                | rpp::ops::as_blocking()
                | rpp::ops::subscribe([&](auto&&) {
                      CHECK(!on_error_called);
                      std::thread{[&] {
                          subject.get_observer().on_error(std::exception_ptr{});
                      }}.detach();
                      std::this_thread::sleep_for(std::chrono::milliseconds{200});
                      CHECK(!on_error_called);
                  } /* on_next */,
                                      [&](auto) {
                                          on_error_called = true;
                                      } /* on_error */);

            CHECK(on_error_called);
        }
    }
}

TEST_CASE("take_until handles current_thread scheduling")
{
    auto mock = mock_observer_strategy<int>{};

    rpp::source::just(rpp::schedulers::current_thread{}, 1, 2, 3)
        | rpp::operators::take_until(rpp::source::interval(std::chrono::seconds{1}, rpp::schedulers::current_thread{}))
        | rpp::operators::subscribe(mock);

    CHECK(mock.get_received_values() == std::vector{1, 2, 3});
    CHECK(mock.get_on_error_count() == 0);
    CHECK(mock.get_on_completed_count() == 1);
}

TEST_CASE("take_until doesn't produce extra copies")
{
    SUBCASE("take_until(other)")
    {
        copy_count_tracker::test_operator(rpp::ops::take_until(rpp::source::never<int>()),
                                          {
                                              .send_by_copy = {.copy_count = 1, // 1 copy to subscriber
                                                               .move_count = 0},
                                              .send_by_move = {.copy_count = 0,
                                                               .move_count = 1} // 1 move to final subscriber
                                          });
    }
}

TEST_CASE("take_until satisfies disposable contracts")
{
    SUBCASE("take_until(never)")
    test_operator_with_disposable<int>(rpp::ops::take_until(rpp::source::never<int>()));
    SUBCASE("take_until(empty)")
    test_operator_finish_before_dispose<int>(rpp::ops::take_until(rpp::source::empty<int>()));
    SUBCASE("never | take_until(ob)")
    test_operator_over_observable_finish_before_dispose<int>([](const auto& ob) {
        return rpp::source::never<int>() | rpp::ops::take_until(ob);
    });
}

TEST_CASE("take_until dispose after completion")
{
    std::vector<std::string> log{};
    rpp::source::empty<int>()
        | rpp::ops::finally([&]() noexcept { log.push_back("finally"); })
        | rpp::ops::subscribe([](int) { }, [&log]() { log.push_back("completed"); });

    CHECK(log == std::vector<std::string>{"completed", "finally"});
}
