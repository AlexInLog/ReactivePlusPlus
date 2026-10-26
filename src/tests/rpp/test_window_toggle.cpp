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
#include <rpp/operators/window_toggle.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/just.hpp>
#include <rpp/sources/never.hpp>

#include "disposable_observable.hpp"
#include "rpp/schedulers/immediate.hpp"


TEST_CASE("window_toggle")
{
    mock_observer_strategy<rpp::window_toggle_observable<int>> mock{};
    std::vector<mock_observer_strategy<int>>                   inner_mocks{};

    auto subscribe_mocks = [&mock, &inner_mocks](const auto& observable) {
        observable.subscribe([&mock, &inner_mocks](const rpp::window_toggle_observable<int>& observable) {
            mock.on_next(observable);
            observable.subscribe(inner_mocks.emplace_back()); },
                             [&mock](const std::exception_ptr& err) { mock.on_error(err); },
                             [&mock]() { mock.on_completed(); });
    };

    SUBCASE("opening - just(1), closing - never()")
    {
        subscribe_mocks(rpp::source::just(1, 2, 3)
                        | rpp::ops::window_toggle(rpp::source::just(1), [](int) { return rpp::source::never<int>(); }));

        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
        REQUIRE(inner_mocks.size() == 1);
        for (const auto& inner : inner_mocks)
        {
            CHECK(inner.get_received_values() == std::vector<int>{1, 2, 3});
            CHECK(inner.get_on_error_count() == 0);
            CHECK(inner.get_on_completed_count() == 1);
        }
    }
    SUBCASE("opening - just(1), closing - empty()")
    {
        subscribe_mocks(rpp::source::just(1, 2, 3)
                        | rpp::ops::window_toggle(rpp::source::just(1), [](int) { return rpp::source::empty<int>(); }));

        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
        REQUIRE(inner_mocks.size() == 1);
        for (const auto& inner : inner_mocks)
        {
            CHECK(inner.get_received_values() == std::vector<int>{});
            CHECK(inner.get_on_error_count() == 0);
            CHECK(inner.get_on_completed_count() == 1);
        }
    }
    SUBCASE("opening - just(1,2,3), closing - empty()")
    {
        subscribe_mocks(rpp::source::just(1, 2, 3)
                        | rpp::ops::window_toggle(rpp::source::just(1, 2, 3), [](int) { return rpp::source::empty<int>(); }));

        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
        REQUIRE(inner_mocks.size() == 3);
        for (const auto& inner : inner_mocks)
        {
            CHECK(inner.get_received_values() == std::vector<int>{});
            CHECK(inner.get_on_error_count() == 0);
            CHECK(inner.get_on_completed_count() == 1);
        }
    }
    SUBCASE("opening - just(1,2,3), closing - never()")
    {
        subscribe_mocks(rpp::source::just(1, 2, 3)
                        | rpp::ops::window_toggle(rpp::source::just(1, 2, 3), [](int) { return rpp::source::never<int>(); }));

        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
        REQUIRE(inner_mocks.size() == 3);
        for (const auto& inner : inner_mocks)
        {
            CHECK(inner.get_on_error_count() == 0);
            CHECK(inner.get_on_completed_count() == 1);
        }
        CHECK(inner_mocks[0].get_received_values() == std::vector<int>{1, 2, 3});
        CHECK(inner_mocks[1].get_received_values() == std::vector<int>{2, 3});
        CHECK(inner_mocks[2].get_received_values() == std::vector<int>{3});
    }
    SUBCASE("opening - just(1,2,3), closing - just(1)")
    {
        subscribe_mocks(rpp::source::just(1, 2, 3)
                        | rpp::ops::window_toggle(rpp::source::just(1, 2, 3), [](int) { return rpp::source::just(1); }));

        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
        REQUIRE(inner_mocks.size() == 3);
        for (const auto& inner : inner_mocks)
        {
            CHECK(inner.get_on_error_count() == 0);
            CHECK(inner.get_on_completed_count() == 1);
        }
        CHECK(inner_mocks[0].get_received_values() == std::vector<int>{1});
        CHECK(inner_mocks[1].get_received_values() == std::vector<int>{2});
        CHECK(inner_mocks[2].get_received_values() == std::vector<int>{3});
    }
    SUBCASE("opening - never(), closing - just(1)")
    {
        subscribe_mocks(rpp::source::never<int>()
                        | rpp::ops::window_toggle(rpp::source::just(1, 2, 3), [](int) { return rpp::source::just(1); }));

        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 0);
        REQUIRE(inner_mocks.size() == 3);
        for (const auto& inner : inner_mocks)
        {
            CHECK(inner.get_on_error_count() == 0);
            CHECK(inner.get_on_completed_count() == 1);
        }
    }
    SUBCASE("opening - empty(), closing - just(1)")
    {
        subscribe_mocks(rpp::source::empty<int>()
                        | rpp::ops::window_toggle(rpp::source::just(1, 2, 3), [](int) { return rpp::source::just(1); }));

        CHECK(mock.get_on_error_count() == 0);
        CHECK(mock.get_on_completed_count() == 1);
        REQUIRE(inner_mocks.size() == 0);
    }
    SUBCASE("source - error")
    {
        subscribe_mocks(rpp::source::error<int>({})
                        | rpp::ops::window_toggle(rpp::source::just(rpp::schedulers::immediate{}, 1), [](int) { return rpp::source::never<int>(); }));

        CHECK(mock.get_on_error_count() == 1);
        CHECK(mock.get_on_completed_count() == 0);
    }
    SUBCASE("openings - error")
    {
        subscribe_mocks(rpp::source::never<int>()
                        | rpp::ops::window_toggle(rpp::source::error<int>({}), [](int) { return rpp::source::never<int>(); }));

        CHECK(mock.get_on_error_count() == 1);
        CHECK(mock.get_on_completed_count() == 0);
    }
    SUBCASE("openings - just(1), closings - error")
    {
        subscribe_mocks(rpp::source::never<int>()
                        | rpp::ops::window_toggle(rpp::source::just(1), [](int) { return rpp::source::error<int>({}); }));

        CHECK(mock.get_on_error_count() == 1);
        CHECK(mock.get_on_completed_count() == 0);
        REQUIRE(inner_mocks.size() == 1);
        for (const auto& inner : inner_mocks)
        {
            CHECK(inner.get_on_error_count() == 1);
            CHECK(inner.get_on_completed_count() == 0);
        }
    }
    SUBCASE("openings - just(1), closings - throw")
    {
        subscribe_mocks(rpp::source::never<int>()
                        | rpp::ops::window_toggle(rpp::source::just(1), [](int) { throw std::runtime_error{""}; return rpp::source::error<int>({}); }));

        CHECK(mock.get_on_error_count() == 1);
        CHECK(mock.get_on_completed_count() == 0);
        REQUIRE(inner_mocks.size() == 1);
        for (const auto& inner : inner_mocks)
        {
            CHECK(inner.get_on_error_count() == 1);
            CHECK(inner.get_on_completed_count() == 0);
        }
    }
}

TEST_CASE("window_toggle disposes original disposable only when everything is disposed")
{

    auto make_observable = [](auto d, bool emit = true) {
        return rpp::source::create<int>([d, emit](auto&& obs) {
            obs.set_upstream(d);
            if (emit)
                obs.on_next(1);
        });
    };

    auto source_disposable  = rpp::composite_disposable_wrapper::make();
    auto opening_disposable = rpp::composite_disposable_wrapper::make();
    auto closing_disposable = rpp::composite_disposable_wrapper::make();

    auto observer_disposable       = rpp::composite_disposable_wrapper::make();
    auto inner_observer_disposable = rpp::composite_disposable_wrapper::make();
    make_observable(source_disposable)
        | rpp::ops::window_toggle(make_observable(opening_disposable), [&](int) { return make_observable(closing_disposable, false); })
        | rpp::ops::subscribe(rpp::composite_disposable_wrapper{observer_disposable}, [inner_observer_disposable](const rpp::window_toggle_observable<int>& new_obs) {
              new_obs.subscribe(rpp::composite_disposable_wrapper{inner_observer_disposable}, [](int) { });
          });

    CHECK(closing_disposable.is_disposed());
    CHECK(!source_disposable.is_disposed());
    CHECK(!observer_disposable.is_disposed());
    CHECK(!inner_observer_disposable.is_disposed());

    observer_disposable.dispose();

    CHECK(observer_disposable.is_disposed());
    CHECK(!opening_disposable.is_disposed());
    CHECK(closing_disposable.is_disposed());
    CHECK(!source_disposable.is_disposed());
    CHECK(!inner_observer_disposable.is_disposed());

    inner_observer_disposable.dispose();

    CHECK(source_disposable.is_disposed());
    CHECK(observer_disposable.is_disposed());
    CHECK(opening_disposable.is_disposed());
    CHECK(closing_disposable.is_disposed());
    CHECK(inner_observer_disposable.is_disposed());
}

TEST_CASE("window_toggle satisfies disposable contracts")
{
    test_operator_with_disposable<int>(rpp::ops::window_toggle(rpp::source::just(1), [](int) { return rpp::source::just(1); }));
}
