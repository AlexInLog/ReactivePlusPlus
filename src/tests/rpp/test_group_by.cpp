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
#include <rpp/operators/delay.hpp>
#include <rpp/operators/group_by.hpp>
#include <rpp/operators/take.hpp>
#include <rpp/schedulers/immediate.hpp>
#include <rpp/sources/create.hpp>
#include <rpp/sources/just.hpp>

#include "disposable_observable.hpp"
#include "rpp/disposables/composite_disposable.hpp"
#include "rpp/disposables/fwd.hpp"

#include <functional>

TEST_CASE("group_by emits grouped seqences of values with identity key selector")
{
    auto                                       obs = mock_observer_strategy<rpp::grouped_observable_group_by<int, int>>{};
    std::map<int, mock_observer_strategy<int>> grouped_mocks{};
    auto                                       observable = rpp::source::just(1, 2, 3, 4, 4, 3, 2, 1) | rpp::operators::group_by(std::identity{});

    SUBCASE("Obtained same amount of grouped observables as amount of unique values")
    {
        observable.subscribe(obs);
        REQUIRE(obs.get_total_on_next_count() == 4);
        REQUIRE(obs.get_on_error_count() == 0);
        REQUIRE(obs.get_on_completed_count() == 1);
    }
    SUBCASE("each grouped observable emits only same values")
    {
        observable.subscribe([&](const auto& grouped) {
            REQUIRE(grouped_mocks.contains(grouped.get_key()) == false);
            grouped.subscribe(grouped_mocks[grouped.get_key()]);
        });

        REQUIRE(grouped_mocks.size() == 4);
        for (const auto& [key, observer] : grouped_mocks)
        {
            REQUIRE(rpp::utils::all_of(observer.get_received_values(), [key = key](int v) { return v == key; }));
            REQUIRE(observer.get_total_on_next_count() == 2);
            REQUIRE(observer.get_on_error_count() == 0);
            REQUIRE(observer.get_on_completed_count() == 1);
        }
    }
    SUBCASE("grouped observables with key 4 unsubscribed early")
    {
        observable.subscribe([&](const auto& grouped) {
            auto key = grouped.get_key();
            REQUIRE(grouped_mocks.contains(key) == false);

            if (key == 4)
                grouped | rpp::operators::take(1) | rpp::operators::subscribe(grouped_mocks[key]);
            else
                grouped.subscribe(grouped_mocks[key]);
        });

        SUBCASE("all except of key 4 obtains as before, but key 4 obtained once")
        {
            REQUIRE(grouped_mocks.size() == 4);
            for (const auto& [key, observer] : grouped_mocks)
            {
                REQUIRE(rpp::utils::all_of(observer.get_received_values(), [key = key](int v) { return v == key; }));
                if (key == 4)
                    REQUIRE(observer.get_total_on_next_count() == 1);
                else
                    REQUIRE(observer.get_total_on_next_count() == 2);
                REQUIRE(observer.get_on_error_count() == 0);
                REQUIRE(observer.get_on_completed_count() == 1);
            }
        }
    }
    SUBCASE("subscribe only on one grouped observable and unsubcribe from root")
    {
        observable | rpp::ops::take(1) | rpp::ops::subscribe([&](const auto& grouped) {
            grouped.subscribe(grouped_mocks[grouped.get_key()]);
        });
        SUBCASE("values for such a observable are still obtainable")
        {
            REQUIRE(grouped_mocks.size() == 1);

            for (const auto& [key, observer] : grouped_mocks)
            {
                REQUIRE(rpp::utils::all_of(observer.get_received_values(), [key = key](int v) { return v == key; }));
                REQUIRE(observer.get_total_on_next_count() == 2);
                REQUIRE(observer.get_on_error_count() == 0);
                REQUIRE(observer.get_on_completed_count() == 1);
            }
        }
    }
}

TEST_CASE("group_by keeps subscription till anyone subscribed")
{
    auto                                      observable_upstream = rpp::composite_disposable_wrapper::make();
    std::optional<rpp::dynamic_observer<int>> extracted{};

    auto observable = rpp::source::create<int>([&](auto&& obs) {
                          obs.set_upstream(rpp::disposable_wrapper{observable_upstream});
                          obs.on_next(1);
                          obs.on_next(2);
                          extracted.emplace(std::forward<decltype(obs)>(obs).as_dynamic());
                      })
                    | rpp::ops::group_by(std::identity{});

    std::vector<rpp::composite_disposable_wrapper> disposables{};
    size_t                                         on_error_count     = 0;
    size_t                                         on_completed_count = 0;
    auto                                           on_error           = [&](const std::exception_ptr&) {
        ++on_error_count;
    };
    auto on_completed = [&]() {
        ++on_completed_count;
    };
    auto d = rpp::composite_disposable_wrapper::make();

    observable.subscribe(
        d,
        [&](const auto& observable) {
            auto d = rpp::composite_disposable_wrapper::make();
            observable.subscribe(
                d,
                [](auto) { },
                on_error,
                on_completed);
            disposables.push_back(d);
        },
        on_error,
        on_completed);

    REQUIRE(extracted.has_value());
    REQUIRE(!d.is_disposed());
    REQUIRE(!observable_upstream.is_disposed());
    REQUIRE(disposables.size() == 2);
    REQUIRE(rpp::utils::all_of(disposables, [](const auto& d) { return !d.is_disposed(); }));
    SUBCASE("dispose root")
    {
        d.dispose();
        REQUIRE(rpp::utils::all_of(disposables, [](const auto& d) { return !d.is_disposed(); }));
        REQUIRE(!observable_upstream.is_disposed());
    }
    SUBCASE("disposing other disposables")
    {
        rpp::utils::for_each(disposables, std::mem_fn(&rpp::composite_disposable_wrapper::dispose));
        REQUIRE(!d.is_disposed());
        REQUIRE(!observable_upstream.is_disposed());
    }
    SUBCASE("dispose all")
    {
        d.dispose();
        rpp::utils::for_each(disposables, std::mem_fn(&rpp::composite_disposable_wrapper::dispose));
        REQUIRE(observable_upstream.is_disposed());
        REQUIRE(d.is_disposed());
        REQUIRE(rpp::utils::all_of(disposables, [](const auto& d) { return d.is_disposed(); }));
    }
    SUBCASE("send on_error")
    {
        extracted->on_error(std::make_exception_ptr(std::runtime_error{""}));
        REQUIRE(d.is_disposed());
        REQUIRE(observable_upstream.is_disposed());
        REQUIRE(rpp::utils::all_of(disposables, [](const auto& d) { return d.is_disposed(); }));
        REQUIRE(on_error_count == disposables.size() + 1);
    }
    SUBCASE("send on_completed")
    {
        extracted->on_completed();
        REQUIRE(d.is_disposed());
        REQUIRE(observable_upstream.is_disposed());
        REQUIRE(rpp::utils::all_of(disposables, [](const auto& d) { return d.is_disposed(); }));
        REQUIRE(on_completed_count == disposables.size() + 1);
    }
}

TEST_CASE("group_by selectors affects types")
{
    auto obs = rpp::source::just(1, 2, 3, 1, 2, 3);
    SUBCASE("subscribe on observable via group_by with const key selector")
    {
        std::vector<int> keys{};
        obs | rpp::ops::group_by([](int) { return 1; })
            | rpp::ops::subscribe([&](const auto& grouped) {
                  keys.push_back(grouped.get_key());
              });

        SUBCASE("only one unique key obtained")
        {
            REQUIRE(keys == std::vector{1});
        }
    }
    SUBCASE("subscribe on observable via group_by with identity key selector")
    {
        std::vector<int> keys{};
        obs | rpp::ops::group_by(std::identity{})
            | rpp::ops::subscribe([&](const auto& grouped) {
                  keys.push_back(grouped.get_key());
              });

        SUBCASE("all values obtained as keys")
        {
            REQUIRE(keys == std::vector{1, 2, 3});
        }
    }
    SUBCASE("subscribe on observable via group_by with value selector")
    {
        auto mock = mock_observer_strategy<std::string>{};
        obs | rpp::ops::group_by(std::identity{}, [](int v) { return std::to_string(v); })
            | rpp::ops::subscribe([&](const auto& grouped) {
                  grouped.subscribe(mock);
              });

        SUBCASE("grouped observables provides modified values")
        {
            using namespace std::string_literals;

            REQUIRE(mock.get_received_values() == std::vector{"1"s, "2"s, "3"s, "1"s, "2"s, "3"s});
        }
    }
    SUBCASE("subscribe on observable via group_by with custom comparator")
    {
        std::vector<int> keys{};
        obs | rpp::ops::group_by(std::identity{}, std::identity{}, [](int f, int s) {
            return f % 2 < s % 2;
        }) | rpp::ops::subscribe([&](const auto& grouped) {
            keys.push_back(grouped.get_key());
        });

        SUBCASE("only 2 types of keys interpreted as unique")
        {
            REQUIRE(keys == std::vector{1, 2});
        }
    }
    auto mock = mock_observer_strategy<rpp::grouped_observable_group_by<int, int>>{};

    SUBCASE("subscribe on observable via group_by with key selector with exception")
    {
        obs | rpp::ops::group_by([](int) -> int { throw std::runtime_error{""}; })
            | rpp::ops::subscribe(mock);
        SUBCASE("on_error obtained once")
        {
            REQUIRE(mock.get_total_on_next_count() == 0);
            REQUIRE(mock.get_on_error_count() == 1);
            REQUIRE(mock.get_on_completed_count() == 0);
        }
    }

    SUBCASE("subscribe on observable via group_by with value selector with exception")
    {
        obs | rpp::ops::group_by(std::identity{}, [](int) -> int { throw std::runtime_error{""}; })
            | rpp::ops::subscribe(mock);
        SUBCASE("on_error obtained once")
        {
            REQUIRE(mock.get_total_on_next_count() == 1);
            REQUIRE(mock.get_on_error_count() == 1);
            REQUIRE(mock.get_on_completed_count() == 0);
        }
    }
}

TEST_CASE("group_by's disposables tracks 1 dispose per call")
{
    auto mock_0 = mock_observer_strategy<int>{};
    rpp::source::just(1, 2)
        | rpp::ops::group_by([](int) { return 0; })
        | rpp::ops::subscribe([&](const auto& observable) {
              for (size_t i = 0; i < 10; ++i)
              {
                  observable
                      | rpp::ops::take(1)
                      | rpp::ops::delay(std::chrono::seconds{0}, rpp::schedulers::immediate{})
                      | rpp::ops::subscribe([](int) { });
              }

              observable.subscribe(mock_0);
          });

    CHECK(mock_0.get_received_values() == std::vector{1, 2});
}

TEST_CASE("group_by satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::group_by([](int) { return 0; }));
}
