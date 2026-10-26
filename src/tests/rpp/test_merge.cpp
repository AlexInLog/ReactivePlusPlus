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
#include <rpp/operators/merge.hpp>
#include <rpp/schedulers/immediate.hpp>
#include <rpp/schedulers/new_thread.hpp>
#include <rpp/sources/create.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/just.hpp>
#include <rpp/sources/never.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"

#include <stdexcept>
#include <string>

TEST_CASE_TEMPLATE("merge for observable of observables", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    auto mock = mock_observer_strategy<int>();
    SUBCASE("observable of observables")
    {
        auto obs = rpp::source::just<TestType>(rpp::schedulers::immediate{}, rpp::source::just<TestType>(rpp::schedulers::immediate{}, 1), rpp::source::just<TestType>(rpp::schedulers::immediate{}, 2));

        SUBCASE("subscribe on merge of observable")
        {
            obs | rpp::operators::merge() | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values FROM underlying observables")
            {
                CHECK(mock.get_received_values() == std::vector{1, 2});
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }

    SUBCASE("observable of observables with first never")
    {
        auto obs = rpp::source::just<TestType>(rpp::source::never<int>().as_dynamic(), rpp::source::just<TestType>(2).as_dynamic());

        SUBCASE("subscribe on merge of observable")
        {
            obs | rpp::ops::merge() | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from second observable even if first emits nothing")
            {
                CHECK(mock.get_received_values() == std::vector{2});
                CHECK(mock.get_on_completed_count() == 0); // no complete due to first observable sends nothing
            }
        }
    }
    SUBCASE("observable of observables without complete")
    {
        auto obs = rpp::source::create<rpp::dynamic_observable<int>>([](const auto& sub) {
            sub.on_next(rpp::source::just<TestType>(1).as_dynamic());
            sub.on_next(rpp::source::just<TestType>(2).as_dynamic());
        });

        SUBCASE("subscribe on merge of observable")
        {
            obs | rpp::ops::merge() | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from second observable even if first emits nothing")
            {
                CHECK(mock.get_received_values() == std::vector{1, 2});
                CHECK(mock.get_on_completed_count() == 0); // no complete due to root observable is not completed
            }
        }
    }
    SUBCASE("observable of observables with error")
    {
        auto obs = rpp::source::create<rpp::dynamic_observable<int>>([](const auto& sub) {
            sub.on_next(rpp::source::just<TestType>(rpp::schedulers::immediate{}, 1).as_dynamic());
            sub.on_next(rpp::source::error<int>(std::make_exception_ptr(std::runtime_error{""})).as_dynamic());
            sub.on_next(rpp::source::just<TestType>(rpp::schedulers::immediate{}, 2).as_dynamic());
        });

        SUBCASE("subscribe on merge of observable")
        {
            obs | rpp::ops::merge() | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from second observable even if first emits nothing")
            {
                CHECK(mock.get_received_values() == std::vector{1});
                CHECK(mock.get_on_error_count() == 1);
                CHECK(mock.get_on_completed_count() == 0); // no complete due to error
            }
        }
    }
    SUBCASE("observable of observables with error")
    {
        auto obs = rpp::source::create<rpp::dynamic_observable<int>>([](const auto& sub) {
            sub.on_error(std::make_exception_ptr(std::runtime_error{""}));
            sub.on_next(rpp::source::just<TestType>(1).as_dynamic());
        });

        SUBCASE("subscribe on merge of observable")
        {
            obs | rpp::ops::merge() | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values from second observable even if first emits nothing")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 1);
                CHECK(mock.get_on_completed_count() == 0); // no complete due to error
            }
        }
    }
}

TEST_CASE_TEMPLATE("merge_with", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    auto mock = mock_observer_strategy<int>();
    SUBCASE("2 observables")
    {
        auto obs_1 = rpp::source::just<TestType>(1);
        auto obs_2 = rpp::source::just<TestType>(2);

        SUBCASE("subscribe on merge of this observables")
        {
            obs_1 | rpp::ops::merge_with(obs_2) | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values FROM both observables")
            {
                CHECK(mock.get_received_values() == std::vector{1, 2});
                CHECK(mock.get_on_completed_count() == 1);
            }
        }
    }

    SUBCASE("never observable with just observable")
    {
        auto obs_1 = rpp::source::never<int>();
        auto obs_2 = rpp::source::just<TestType>(2);

        SUBCASE("subscribe on merge of this observables")
        {
            auto op = rpp::ops::merge_with(obs_2);
            obs_1 | op | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values FROM both observables")
            {
                CHECK(mock.get_received_values() == std::vector{2});
                CHECK(mock.get_on_completed_count() == 0); // first observable never completes
            }
        }

        SUBCASE("subscribe on merge of this observables in reverse oreder")
        {
            obs_2 | rpp::ops::merge_with(obs_1) | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values FROM both observables")
            {
                CHECK(mock.get_received_values() == std::vector{2});
                CHECK(mock.get_on_completed_count() == 0); // first observable never completes
            }
        }
    }

    SUBCASE("error observable with just observable")
    {
        auto obs_1 = rpp::source::error<int>(std::make_exception_ptr(std::runtime_error{""}));
        auto obs_2 = rpp::source::just<TestType>(2);

        SUBCASE("subscribe on merge of this observables")
        {
            obs_1 | rpp::ops::merge_with(obs_2) | rpp::ops::subscribe(mock);
            SUBCASE("observer obtains values FROM both observables")
            {
                CHECK(mock.get_total_on_next_count() == 0);
                CHECK(mock.get_on_error_count() == 1);
                CHECK(mock.get_on_completed_count() == 0);
            }
        }
    }
}

TEST_CASE_TEMPLATE("merge serializes emissions", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    SUBCASE("observables from different threads")
    {
        auto s1 = rpp::source::just<TestType>(rpp::schedulers::new_thread{}, 1);
        auto s2 = rpp::source::just<TestType>(rpp::schedulers::new_thread{}, 2);
        SUBCASE("subscribe on merge of this observables")
        {
            SUBCASE("resulting observable emits items sequentially")
            {
                std::atomic_size_t counter{};
                size_t             max_value = 0;
                s1 | rpp::ops::merge_with(s2) | rpp::ops::as_blocking() | rpp::ops::subscribe([&](const auto&) {
                    if (++counter >= 2) FAIL("++counter >= 2");
                    max_value = std::max(counter.load(), max_value);

                    std::this_thread::sleep_for(std::chrono::seconds{1});

                    max_value = std::max(counter.load(), max_value);
                    --counter;
                });
                CHECK(max_value == 1);
            }
        }
    }
}

TEST_CASE_TEMPLATE("merge handles race condition", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    SUBCASE("source observable in current thread pairs with error in other thread")
    {
        std::atomic_bool                          on_error_called{false};
        std::optional<rpp::dynamic_observer<int>> extracted_obs{};
        auto                                      delayed_obs = rpp::source::create<int>([&](auto&& obs) {
            extracted_obs.emplace(std::forward<decltype(obs)>(obs).as_dynamic());
        });

        auto test = [&](auto source) {
            SUBCASE("subscribe on it")
            {
                SUBCASE("on_error can't interleave with on_next")
                {
                    std::optional<std::thread> t{};
                    source
                        | rpp::ops::as_blocking()
                        | rpp::ops::subscribe([&](auto&&) {
                                    REQUIRE(extracted_obs.has_value());
                                    if (!t)
                                    {
                                        CHECK(!on_error_called);
                                        t = std::thread{[extracted_obs]
                                        {
                                            extracted_obs->on_error(std::exception_ptr{});
                                        }};
                                        std::this_thread::sleep_for(std::chrono::seconds{1});
                                        CHECK(!on_error_called);
                                    } },
                                              [&](auto) { on_error_called = true; });
                    REQUIRE(t.has_value());
                    t->join();
                    CHECK(on_error_called);
                }
            }
        };
        SUBCASE("just + merge_with")
        test(rpp::source::just<TestType>(1, 1, 1) | rpp::ops::merge_with(delayed_obs));

        SUBCASE("just<TestType>(just) + merge")
        test(rpp::source::just<TestType>(rpp::schedulers::immediate{}, rpp::source::just<TestType>(1, 1, 1).as_dynamic(), delayed_obs.as_dynamic()) | rpp::ops::merge());
    }
}

TEST_CASE("merge dispose inner_disposable immediately")
{
    rpp::source::create<int>([](auto&& d) {
        auto disposable = rpp::composite_disposable_wrapper::make();
        d.set_upstream(rpp::disposable_wrapper{disposable});
        d.on_completed();
        CHECK(disposable.is_disposed());
    })
        | rpp::ops::merge_with(rpp::source::never<int>())
        | rpp::ops::subscribe([](int) { });
}

TEST_CASE("merge is not deadlocking is_disposed")
{
    std::optional<rpp::dynamic_observer<int>> observer{};
    rpp::source::create<int>([&observer](auto&& obs) {
        observer = std::forward<decltype(obs)>(obs).as_dynamic();
        observer->on_next(1);
    })
        | rpp::ops::merge_with(rpp::source::never<int>())
        | rpp::ops::subscribe([&observer](int) {
              CHECK(observer);
              CHECK(!observer->is_disposed());
          });
}

TEST_CASE("merge doesn't produce extra copies")
{
    SUBCASE("send value by copy")
    {
        copy_count_tracker verifier{};
        auto               obs = rpp::source::just(verifier.get_observable()) | rpp::ops::merge();
        obs.subscribe([](copy_count_tracker) { }); // NOLINT
        REQUIRE(verifier.get_copy_count() == 1);   // 1 copy to final subscriber
        REQUIRE(verifier.get_move_count() == 0);
    }

    SUBCASE("send value by move")
    {
        copy_count_tracker verifier{};
        auto               obs = rpp::source::just(verifier.get_observable_for_move()) | rpp::ops::merge();
        obs.subscribe([](copy_count_tracker) { }); // NOLINT
        REQUIRE(verifier.get_copy_count() == 0);
        REQUIRE(verifier.get_move_count() == 1); // 1 move to final subscriber
    }
}

TEST_CASE("merge satisfies disposable contracts")
{
    auto observable_disposable = rpp::composite_disposable_wrapper::make();
    {
        auto observable = observable_with_disposable<int>(observable_disposable);
        auto op         = rpp::ops::merge_with(observable);

        test_operator_with_disposable<int>(op);
        test_operator_finish_before_dispose<int>(op);
    }
    CHECK((observable_disposable.is_disposed() || observable_disposable.lock().use_count() == 2));
}
