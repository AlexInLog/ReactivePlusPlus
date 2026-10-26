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

#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/observers/mock_observer.hpp>
#include <rpp/operators/as_blocking.hpp>
#include <rpp/sources/create.hpp>
#include <rpp/subjects/behavior_subject.hpp>
#include <rpp/subjects/publish_subject.hpp>
#include <rpp/subjects/replay_subject.hpp>

#include "copy_count_tracker.hpp"
#include "rpp_trompeloil.hpp"

#include <thread>

TEST_CASE("publish subject multicasts values")
{
    auto mock_1 = mock_observer_strategy<int>{};
    auto mock_2 = mock_observer_strategy<int>{};
    SUBCASE("publish subject")
    {
        auto sub = rpp::subjects::publish_subject<int>{};
        SUBCASE("subscribe multiple observers")
        {
            auto dis_1 = rpp::composite_disposable_wrapper::make();
            auto dis_2 = rpp::composite_disposable_wrapper::make();
            sub.get_observable().subscribe(mock_1.get_observer(dis_1));
            sub.get_observable().subscribe(mock_2.get_observer(dis_2));

            SUBCASE("emit value")
            {
                sub.get_observer().on_next(1);
                SUBCASE("observers obtain value")
                {
                    auto validate = [](auto mock) {
                        CHECK(mock.get_received_values() == std::vector{1});
                        CHECK(mock.get_total_on_next_count() == 1);
                        CHECK(mock.get_on_error_count() == 0);
                        CHECK(mock.get_on_completed_count() == 0);
                    };
                    validate(mock_1);
                    validate(mock_2);
                }
            }
            SUBCASE("emit error")
            {
                sub.get_observer().on_error(std::make_exception_ptr(std::runtime_error{""}));
                SUBCASE("observers obtain error")
                {
                    auto validate = [](auto mock) {
                        CHECK(mock.get_total_on_next_count() == 0);
                        CHECK(mock.get_on_error_count() == 1);
                        CHECK(mock.get_on_completed_count() == 0);
                    };
                    validate(mock_1);
                    validate(mock_2);
                    SUBCASE("and then emission of on_next does nothing")
                    {
                        sub.get_observer().on_next(1);
                        validate(mock_1);
                        validate(mock_2);
                    }
                }
            }
            SUBCASE("emit on_completed")
            {
                sub.get_observer().on_completed();
                SUBCASE("observers obtain on_completed")
                {
                    auto validate = [](auto mock) {
                        CHECK(mock.get_total_on_next_count() == 0);
                        CHECK(mock.get_on_error_count() == 0);
                        CHECK(mock.get_on_completed_count() == 1);
                    };
                    validate(mock_1);
                    validate(mock_2);

                    SUBCASE("and then emission of on_next does nothing")
                    {
                        sub.get_observer().on_next(1);
                        validate(mock_1);
                        validate(mock_2);
                    }
                }
            }
            SUBCASE("emit multiple values")
            {
                SUBCASE("each sbuscriber obtain first value, then seconds and etc")
                {
                    sub.get_observer().on_next(1);
                    auto check_1 = [](auto mock) {
                        CHECK(mock.get_received_values() == std::vector{1});
                    };
                    check_1(mock_1);
                    check_1(mock_2);

                    sub.get_observer().on_next(2);
                    auto check_2 = [](auto mock) {
                        CHECK(mock.get_received_values() == std::vector{1, 2});
                    };
                    check_2(mock_1);
                    check_2(mock_2);
                }
            }
            SUBCASE("first subscriber unsubscribes and then emit value")
            {
                // 1 native, 1 inside subject
                // CHECK(dis_1.use_count() == 2);
                dis_1.dispose();
                // only this 1 native
                // CHECK(dis_1.use_count() == 1);

                sub.get_observer().on_next(1);
                SUBCASE("observers obtain value")
                {
                    CHECK(mock_1.get_total_on_next_count() == 0);
                    CHECK(mock_1.get_on_error_count() == 0);
                    CHECK(mock_1.get_on_completed_count() == 0);

                    CHECK(mock_2.get_received_values() == std::vector{1});
                    CHECK(mock_2.get_total_on_next_count() == 1);
                    CHECK(mock_2.get_on_error_count() == 0);
                    CHECK(mock_2.get_on_completed_count() == 0);
                }
            }
        }
    }
}

TEST_CASE("subject can be modified from on_next call")
{
    rpp::subjects::publish_subject<int> subject{};
    mock_observer<int>                  inner_mock{};

    SUBCASE("subscribe inside on_next")
    {
        subject.get_observable().subscribe([&subject, &inner_mock](int) {
            subject.get_observable().subscribe(inner_mock);
        });

        subject.get_observer().on_next(1);

        REQUIRE_CALL(*inner_mock, on_next_lvalue(2));
        subject.get_observer().on_next(2);
    }

    SUBCASE("unsubscribe inside on_next")
    {
        auto d = rpp::composite_disposable_wrapper::make();

        subject.get_observable().subscribe([d](int) {
            d.clear();
        });
        subject.get_observable().subscribe(d, inner_mock);

        REQUIRE_CALL(*inner_mock, on_next_lvalue(1));
        subject.get_observer().on_next(1);
        subject.get_observer().on_next(2);
    }
}

TEST_CASE("subject handles addition from inside on_next properly")
{
    rpp::subjects::publish_subject<int> subject{};

    SUBCASE("subscribe inside on_next")
    {
        int value = {};
        subject.get_observable().subscribe([&subject, &value](int v) {
            for (int i = 0; i < 100; ++i)
                subject.get_observable().subscribe([](int) { });
            value = v;
        });

        for (int i = 0; i < 100; ++i)
            subject.get_observer().on_next(i);

        REQUIRE(value == 99);
    }
}

TEST_CASE("publish subject caches error/completed")
{
    auto mock = mock_observer_strategy<int>{};
    SUBCASE("publish subject")
    {
        auto subj = rpp::subjects::publish_subject<int>{};
        SUBCASE("emit value")
        {
            subj.get_observer().on_next(1);
            SUBCASE("subscribe observer after emission")
            {
                subj.get_observable().subscribe(mock);
                SUBCASE("observer doesn't obtain value")
                {
                    CHECK(mock.get_total_on_next_count() == 0);
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 0);
                }
            }
        }
        SUBCASE("emit error")
        {
            subj.get_observer().on_error(std::make_exception_ptr(std::runtime_error{""}));
            SUBCASE("subscribe observer after emission")
            {
                subj.get_observable().subscribe(mock);
                SUBCASE("observer obtains error")
                {
                    CHECK(mock.get_total_on_next_count() == 0);
                    CHECK(mock.get_on_error_count() == 1);
                    CHECK(mock.get_on_completed_count() == 0);
                }
            }
        }
        SUBCASE("emit on_completed")
        {
            subj.get_observer().on_completed();
            SUBCASE("subscribe observer after emission")
            {
                subj.get_observable().subscribe(mock);
                SUBCASE("observer obtains on_completed")
                {
                    CHECK(mock.get_total_on_next_count() == 0);
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 1);
                }
            }
        }
        SUBCASE("emit error and on_completed")
        {
            subj.get_observer().on_error(std::make_exception_ptr(std::runtime_error{""}));
            subj.get_observer().on_completed();
            SUBCASE("subscribe observer after emission")
            {
                subj.get_observable().subscribe(mock);
                SUBCASE("observer obtains error")
                {
                    CHECK(mock.get_total_on_next_count() == 0);
                    CHECK(mock.get_on_error_count() == 1);
                    CHECK(mock.get_on_completed_count() == 0);
                }
            }
        }
        SUBCASE("emit on_completed and error")
        {
            subj.get_observer().on_completed();
            subj.get_observer().on_error(std::make_exception_ptr(std::runtime_error{""}));
            SUBCASE("subscribe observer after emission")
            {
                subj.get_observable().subscribe(mock);
                SUBCASE("observer obtains on_completed")
                {
                    CHECK(mock.get_total_on_next_count() == 0);
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 1);
                }
            }
        }
        SUBCASE("emit everything after on_completed via get_observer to avoid subscription")
        {
            auto observer = subj.get_observer();
            observer.on_completed();
            subj.get_observable().subscribe(mock);
            observer.on_next(1);
            observer.on_error(std::make_exception_ptr(std::runtime_error{""}));
            observer.on_completed();
            SUBCASE("no any calls except of cached on_completed")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 0);
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }
}

TEST_CASE_TEMPLATE("serialized subjects handles race condition", TestType, rpp::subjects::serialized_publish_subject<int>, rpp::subjects::serialized_replay_subject<int>, rpp::subjects::serialized_behavior_subject<int>)
{
    auto subj = []() {
        if constexpr (std::same_as<TestType, rpp::subjects::serialized_behavior_subject<int>>)
            return TestType{0};
        else
            return TestType{};
    }();

    SUBCASE("call on_next from 2 threads")
    {
        bool on_error_called{};
        rpp::source::create<int>([&](auto&& obs) {
            subj.get_observable().subscribe(std::forward<decltype(obs)>(obs));
            subj.get_observer().on_next(1);
        })
            | rpp::operators::as_blocking()
            | rpp::operators::subscribe([&](int) {
            CHECK(!on_error_called);
            std::thread{[&]{ subj.get_observer().on_error({}); }}.detach();

            std::this_thread::sleep_for(std::chrono::seconds{1});
            CHECK(!on_error_called); },
                                        [&](const std::exception_ptr&) { on_error_called = true; });

        CHECK(on_error_called);
    }
}

TEST_CASE_TEMPLATE("replay subject multicasts values and replay", TestType, rpp::subjects::replay_subject<int>, rpp::subjects::serialized_replay_subject<int>)
{
    SUBCASE("replay subject")
    {
        auto mock_1 = mock_observer_strategy<int>{};
        auto mock_2 = mock_observer_strategy<int>{};
        auto mock_3 = mock_observer_strategy<int>{};

        auto sub = TestType{};

        SUBCASE("subscribe multiple observers")
        {
            sub.get_observable().subscribe(mock_1.get_observer());
            sub.get_observable().subscribe(mock_2.get_observer());

            sub.get_observer().on_next(1);
            sub.get_observer().on_next(2);
            sub.get_observer().on_next(3);

            SUBCASE("observers obtain values")
            {
                auto validate = [](auto mock) {
                    CHECK(mock.get_received_values() == std::vector{1, 2, 3});
                    CHECK(mock.get_total_on_next_count() == 3);
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 0);
                };
                validate(mock_1);
                validate(mock_2);
            }

            sub.get_observable().subscribe(mock_3.get_observer());

            SUBCASE("observer obtains replayed values")
            {
                CHECK(mock_3.get_received_values() == std::vector{1, 2, 3});
                CHECK(mock_3.get_total_on_next_count() == 3);
                CHECK(mock_3.get_on_error_count() == 0);
                CHECK(mock_3.get_on_completed_count() == 0);
            }

            sub.get_observer().on_next(4);

            SUBCASE("observers stil obtain values")
            {
                auto validate = [](auto mock) {
                    CHECK(mock.get_received_values() == std::vector{1, 2, 3, 4});
                    CHECK(mock.get_total_on_next_count() == 4);
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 0);
                };
                validate(mock_1);
                validate(mock_2);
                validate(mock_3);
            }
        }
    }

    SUBCASE("bounded replay subject")
    {
        auto mock_1 = mock_observer_strategy<int>{};
        auto mock_2 = mock_observer_strategy<int>{};

        size_t bound = 1;
        auto   sub   = TestType{bound};

        SUBCASE("subscribe multiple observers")
        {
            sub.get_observable().subscribe(mock_1.get_observer());

            sub.get_observer().on_next(1);
            sub.get_observer().on_next(2);
            sub.get_observer().on_next(3);

            SUBCASE("observer obtains values")
            {
                CHECK(mock_1.get_received_values() == std::vector{1, 2, 3});
                CHECK(mock_1.get_total_on_next_count() == 3);
                CHECK(mock_1.get_on_error_count() == 0);
                CHECK(mock_1.get_on_completed_count() == 0);
            }

            sub.get_observable().subscribe(mock_2.get_observer());

            SUBCASE("observer obtains latest replayed values")
            {
                CHECK(mock_2.get_received_values() == std::vector{3});
                CHECK(mock_2.get_total_on_next_count() == 1);
                CHECK(mock_2.get_on_error_count() == 0);
                CHECK(mock_2.get_on_completed_count() == 0);
            }
        }
    }

    SUBCASE("bounded replay subject with duration")
    {
        using namespace std::chrono_literals;

        auto mock_1 = mock_observer_strategy<int>{};
        auto mock_2 = mock_observer_strategy<int>{};

        size_t bound    = 2;
        auto   duration = 5ms;
        auto   sub      = TestType{bound, duration};

        SUBCASE("subscribe multiple observers")
        {
            sub.get_observable().subscribe(mock_1.get_observer());

            sub.get_observer().on_next(1);
            sub.get_observer().on_next(2);
            sub.get_observer().on_next(3);

            SUBCASE("observer obtains values")
            {
                CHECK(mock_1.get_received_values() == std::vector{1, 2, 3});
                CHECK(mock_1.get_total_on_next_count() == 3);
                CHECK(mock_1.get_on_error_count() == 0);
                CHECK(mock_1.get_on_completed_count() == 0);
            }

            std::this_thread::sleep_for(duration);

            sub.get_observable().subscribe(mock_2.get_observer());

            SUBCASE("subject replay only non expired values")
            {
                CHECK(mock_2.get_received_values() == std::vector<int>{});
                CHECK(mock_2.get_total_on_next_count() == 0);
                CHECK(mock_2.get_on_error_count() == 0);
                CHECK(mock_2.get_on_completed_count() == 0);
            }
        }
    }
}

TEST_CASE_TEMPLATE("replay subject doesn't introduce additional copies", TestType, rpp::subjects::replay_subject<copy_count_tracker>, rpp::subjects::serialized_replay_subject<copy_count_tracker>)
{
    SUBCASE("on_next by rvalue")
    {
        auto sub = TestType{};

        sub.get_observable().subscribe([](copy_count_tracker tracker) { // NOLINT
            CHECK(tracker.get_copy_count() == 2);                       // 1 copy to internal replay buffer + 1 copy to this observer
            CHECK(tracker.get_move_count() == 0);
        });

        sub.get_observer().on_next(copy_count_tracker{});

        sub.get_observable().subscribe([](copy_count_tracker tracker) { // NOLINT
            CHECK(tracker.get_copy_count() == 2 + 1);                   // + 1 copy values from buffer for this observer
            CHECK(tracker.get_move_count() == 0 + 1);                   // + 1 move to this observer
        });
    }

    SUBCASE("on_next by lvalue")
    {
        copy_count_tracker tracker{};
        auto               sub = TestType{};

        sub.get_observable().subscribe([](copy_count_tracker tracker) { // NOLINT
            CHECK(tracker.get_copy_count() == 2);                       // 1 copy to internal replay buffer + 1 copy to this observer
            CHECK(tracker.get_move_count() == 0);
        });

        sub.get_observer().on_next(tracker);

        sub.get_observable().subscribe([](copy_count_tracker tracker) { // NOLINT
            CHECK(tracker.get_copy_count() == 2 + 1);                   // + 1 copy values from buffer for this observer
            CHECK(tracker.get_move_count() == 0 + 1);                   // + 1 move to this observer
        });
    }
}

TEST_CASE_TEMPLATE("replay subject multicasts values and replay", TestType, rpp::subjects::behavior_subject<int>, rpp::subjects::serialized_behavior_subject<int>)
{
    const auto mock_1 = mock_observer_strategy<int>{};
    const auto subj   = TestType{10};

    CHECK(subj.get_value() == 10);

    SUBCASE("subscribe to subject with default")
    {
        subj.get_observable().subscribe(mock_1);
        CHECK(mock_1.get_received_values() == std::vector<int>{10});

        SUBCASE("emit value and subscribe other observer")
        {
            const auto mock_2 = mock_observer_strategy<int>{};

            subj.get_observer().on_next(5);
            CHECK(subj.get_value() == 5);

            CHECK(mock_1.get_received_values() == std::vector<int>{10, 5});
            CHECK(mock_2.get_received_values() == std::vector<int>{});

            subj.get_observable().subscribe(mock_2);

            CHECK(mock_2.get_received_values() == std::vector<int>{5});

            SUBCASE("emit one more value and subscribe one more other observer")
            {
                const auto mock_3 = mock_observer_strategy<int>{};
                subj.get_observer().on_next(1);
                CHECK(subj.get_value() == 1);

                CHECK(mock_1.get_received_values() == std::vector<int>{10, 5, 1});
                CHECK(mock_2.get_received_values() == std::vector<int>{5, 1});
                CHECK(mock_3.get_received_values() == std::vector<int>{});

                subj.get_observable().subscribe(mock_3);

                CHECK(mock_3.get_received_values() == std::vector<int>{1});
            }
        }

        SUBCASE("subject keeps error")
        {
            subj.get_observer().on_error(std::exception_ptr{});
            CHECK(mock_1.get_on_error_count() == 1);

            const auto mock_4 = mock_observer_strategy<int>{};
            subj.get_observable().subscribe(mock_4);

            CHECK(mock_4.get_received_values() == std::vector<int>{});
            CHECK(mock_4.get_on_error_count() == 1);
            CHECK(mock_4.get_on_completed_count() == 0);
        }
    }
}
