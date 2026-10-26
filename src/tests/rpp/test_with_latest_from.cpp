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
#include <rpp/operators/with_latest_from.hpp>
#include <rpp/sources/just.hpp>
#include <rpp/subjects/publish_subject.hpp>

#include "disposable_observable.hpp"


TEST_CASE("with_latest_from combines observables")
{
    auto obs_1 = rpp::source::just(1);
    auto obs_2 = rpp::source::just(2.2);
    SUBCASE("subscribe on it via with_latest_from")
    {
        auto mock = mock_observer_strategy<std::tuple<int, double>>{};
        obs_1 | rpp::ops::with_latest_from(obs_2) | rpp::ops::subscribe(mock);
        SUBCASE("obtain tuple of values")
        {
            CHECK(mock.get_received_values() == std::vector{std::tuple{1, 2.2}});
            CHECK(mock.get_total_on_next_count() == 1);
            CHECK(mock.get_on_completed_count() == 1);
        }
    }
    SUBCASE("subscribe on it via with_latest_from with custom selector")
    {
        auto mock = mock_observer_strategy<double>{};
        obs_1 | rpp::ops::with_latest_from([](int left, double right) { return left + right; }, obs_2) | rpp::ops::subscribe(mock);
        SUBCASE("obtain values")
        {
            CHECK(mock.get_received_values() == std::vector{1 + 2.2});
            CHECK(mock.get_total_on_next_count() == 1);
            CHECK(mock.get_on_completed_count() == 1);
        }
    }
}

TEST_CASE("with_latest_from reacts only on main root but sends last value from others")
{
    SUBCASE("subjects and subscribe on it with_latest_from")
    {
        auto subj_1 = rpp::subjects::publish_subject<int>{};
        auto subj_2 = rpp::subjects::publish_subject<int>{};
        auto mock   = mock_observer_strategy<std::tuple<int, int>>{};
        subj_1.get_observable() | rpp::ops::with_latest_from(subj_2.get_observable()) | rpp::ops::subscribe(mock);
        SUBCASE("send only first subject sends value")
        {
            subj_1.get_observer().on_next(1);
            SUBCASE("No values to observer")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
        SUBCASE("send only second subject sends value")
        {
            subj_2.get_observer().on_next(1);
            SUBCASE("No values to observer")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
        SUBCASE("send first subject sends value SUBCASE combine called with last second value")
        {
            subj_2.get_observer().on_next(1);
            subj_2.get_observer().on_next(2);
            subj_2.get_observer().on_next(3);
            subj_1.get_observer().on_next(4);
            SUBCASE("obtain last combintaion")
            {
                CHECK(mock.get_received_values() == std::vector{std::tuple{4, 3}});
                CHECK(mock.get_total_on_next_count() == 1);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
            }
            SUBCASE("second sends values again")
            {
                subj_2.get_observer().on_next(5);
                SUBCASE("nothing new happens")
                {
                    CHECK(mock.get_received_values() == std::vector{std::tuple{4, 3}});
                    CHECK(mock.get_total_on_next_count() == 1);
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 0);
                }
            }
        }
        SUBCASE("second completes")
        {
            subj_2.get_observer().on_completed();
            SUBCASE("nothing happens")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
        SUBCASE("second errors")
        {
            subj_2.get_observer().on_error({});
            SUBCASE("error obtained")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 1);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
        SUBCASE("first completes")
        {
            subj_1.get_observer().on_completed();
            SUBCASE("observer completed")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
        SUBCASE("first errors")
        {
            subj_1.get_observer().on_error({});
            SUBCASE("error obtained")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 1);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
    }
}

TEST_CASE("with_latest_from handles race condition")
{
    SUBCASE("source observable in current thread pairs with error in other thread")
    {
        std::atomic_bool on_error_called{false};
        auto             subject = rpp::subjects::publish_subject<int>{};

        SUBCASE("subscribe on it")
        {
            SUBCASE("on_error can't interleave with on_next")
            {
                std::thread th{};
                auto        source = rpp::subjects::publish_subject<int>{};

                source.get_observable()
                    | rpp::ops::with_latest_from(subject.get_observable())
                    | rpp::ops::subscribe([&](auto&&) {
                                     CHECK(!on_error_called);
                                     th = std::thread{[&]
                                     {
                                         subject.get_observer().on_error(std::exception_ptr{});
                                     }};
                                     std::this_thread::sleep_for(std::chrono::seconds{1});
                                     CHECK(!on_error_called); },
                                          [&](auto) { on_error_called = true; });

                subject.get_observer().on_next(2);
                source.get_observer().on_next(1);

                th.join();

                CHECK(on_error_called);
            }
        }
    }
}


TEST_CASE("with_latest_from handles current_thread scheduling")
{
    auto mock = mock_observer_strategy<std::tuple<int, int>>{};

    rpp::source::just(rpp::schedulers::current_thread{}, 1, 2, 3)
        | rpp::operators::with_latest_from(rpp::source::just(rpp::schedulers::current_thread{}, 1, 2, 3))
        | rpp::operators::subscribe(mock);

    CHECK(mock.get_received_values() == std::vector{std::tuple{1, 1}, std::tuple{2, 2}, std::tuple{3, 3}});
    CHECK(mock.get_on_error_count() == 0);
    CHECK(mock.get_on_completed_count() == 1);
}

TEST_CASE("with_latest_from satisfies disposable contracts")
{
    auto observable_disposable = rpp::composite_disposable_wrapper::make();
    {
        auto observable = observable_with_disposable<int>(observable_disposable);
        auto op         = rpp::ops::with_latest_from(observable);

        test_operator_with_disposable<int>(op);
        test_operator_finish_before_dispose<int>(op);
    }

    CHECK((observable_disposable.is_disposed() || observable_disposable.lock().use_count() == 2));
}
