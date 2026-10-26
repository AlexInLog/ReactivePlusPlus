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

#include <rpp/observers/dynamic_observer.hpp>
#include <rpp/observers/mock_observer.hpp>
#include <rpp/sources/just.hpp>

#include <exception>

TEST_CASE_TEMPLATE("subscribe as operator", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    mock_observer_strategy<int> mock{};
    auto                        observable = rpp::source::just<TestType>(1);

    SUBCASE("subscribe observer strategy")
    {
        static_assert(std::is_same_v<decltype(observable | rpp::operators::subscribe(mock)), void>);
        observable | rpp::operators::subscribe(mock);
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe observer strategy with disposable")
    {
        static_assert(std::is_same_v<decltype(observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::make(), mock)), rpp::composite_disposable_wrapper>);
        auto d = observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::make(), mock);
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe observer strategy with disposed disposable")
    {
        static_assert(std::is_same_v<decltype(observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::empty(), mock)), rpp::composite_disposable_wrapper>);
        auto d = observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::empty(), mock);
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values().empty());
    }

    SUBCASE("subscribe observer")
    {
        static_assert(std::is_same_v<decltype(observable | rpp::operators::subscribe(mock.get_observer())), void>);
        observable | rpp::operators::subscribe(mock.get_observer());
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe observer with disposable")
    {
        static_assert(std::is_same_v<decltype(observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::make(), mock.get_observer())), rpp::composite_disposable_wrapper>);
        auto d = observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::make(), mock.get_observer());
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe observer with disposed disposable")
    {
        static_assert(std::is_same_v<decltype(observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::empty(), mock.get_observer())), rpp::composite_disposable_wrapper>);
        auto d = observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::empty(), mock.get_observer());
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values().empty());
    }

    SUBCASE("subscribe dynamic observer")
    {
        static_assert(std::is_same_v<decltype(observable | rpp::operators::subscribe(mock.get_observer().as_dynamic())), void>);
        observable | rpp::operators::subscribe(mock.get_observer().as_dynamic());
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe dynamic observer with disposable")
    {
        static_assert(std::is_same_v<decltype(observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::make(), mock.get_observer().as_dynamic())), rpp::composite_disposable_wrapper>);
        auto d = observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::make(), mock.get_observer().as_dynamic());
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe dynamic observer with disposed disposable")
    {
        static_assert(std::is_same_v<decltype(observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::empty(), mock.get_observer().as_dynamic())), rpp::composite_disposable_wrapper>);
        auto d = observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::empty(), mock.get_observer().as_dynamic());
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values().empty());
    }

    SUBCASE("subscribe lambdas")
    {
        static_assert(std::is_same_v<decltype(observable | rpp::operators::subscribe(rpp::utils::empty_function_t<int>{}, rpp::utils::empty_function_t<std::exception_ptr>{}, rpp::utils::empty_function_t<>{})), void>);
        observable | rpp::operators::subscribe([&mock](const auto& v) { mock.on_next(v); }, rpp::utils::empty_function_t<std::exception_ptr>{}, rpp::utils::empty_function_t<>{});
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe lambdas with disposable")
    {
        static_assert(std::is_same_v<decltype(observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::make(), rpp::utils::empty_function_t<int>{}, rpp::utils::empty_function_t<std::exception_ptr>{}, rpp::utils::empty_function_t<>{})), rpp::composite_disposable_wrapper>);
        auto d = observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::make(), [&mock](const auto& v) { mock.on_next(v); }, rpp::utils::empty_function_t<std::exception_ptr>{}, rpp::utils::empty_function_t<>{});
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe lambdas with disposed disposable")
    {
        static_assert(std::is_same_v<decltype(observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::empty(), rpp::utils::empty_function_t<int>{}, rpp::utils::empty_function_t<std::exception_ptr>{}, rpp::utils::empty_function_t<>{})), rpp::composite_disposable_wrapper>);
        auto d = observable | rpp::operators::subscribe(rpp::composite_disposable_wrapper::empty(), [&mock](const auto& v) { mock.on_next(v); }, rpp::utils::empty_function_t<std::exception_ptr>{}, rpp::utils::empty_function_t<>{});
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values().empty());
    }
}

TEST_CASE_TEMPLATE("subscribe as member", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    mock_observer_strategy<int> mock{};
    auto                        observable = rpp::source::just<TestType>(1);

    SUBCASE("subscribe observer strategy")
    {
        static_assert(std::is_same_v<decltype(observable.subscribe(mock)), void>);
        observable.subscribe(mock);
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe observer strategy with disposable")
    {
        static_assert(std::is_same_v<decltype(observable.subscribe(rpp::composite_disposable_wrapper::make(), mock)), rpp::composite_disposable_wrapper>);
        auto d = observable.subscribe(rpp::composite_disposable_wrapper::make(), mock);
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe observer strategy with disposed disposable")
    {
        static_assert(std::is_same_v<decltype(observable.subscribe(rpp::composite_disposable_wrapper::empty(), mock)), rpp::composite_disposable_wrapper>);
        auto d = observable.subscribe(rpp::composite_disposable_wrapper::empty(), mock);
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values().empty());
    }

    SUBCASE("subscribe observer")
    {
        static_assert(std::is_same_v<decltype(observable.subscribe(mock.get_observer())), void>);
        observable.subscribe(mock);
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe observer with disposable")
    {
        static_assert(std::is_same_v<decltype(observable.subscribe(rpp::composite_disposable_wrapper::make(), mock.get_observer())), rpp::composite_disposable_wrapper>);
        auto d = observable.subscribe(rpp::composite_disposable_wrapper::make(), mock.get_observer());
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe observer with disposed disposable")
    {
        static_assert(std::is_same_v<decltype(observable.subscribe(rpp::composite_disposable_wrapper::empty(), mock.get_observer())), rpp::composite_disposable_wrapper>);
        auto d = observable.subscribe(rpp::composite_disposable_wrapper::empty(), mock.get_observer());
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values().empty());
    }

    SUBCASE("subscribe dynamic observer")
    {
        static_assert(std::is_same_v<decltype(observable.subscribe(mock.get_observer().as_dynamic())), void>);
        observable.subscribe(mock.get_observer().as_dynamic());
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe dynamic observer with disposable")
    {
        static_assert(std::is_same_v<decltype(observable.subscribe(rpp::composite_disposable_wrapper::make(), mock.get_observer().as_dynamic())), rpp::composite_disposable_wrapper>);
        auto d = observable.subscribe(rpp::composite_disposable_wrapper::make(), mock.get_observer().as_dynamic());
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe dynamic observer with disposed disposable")
    {
        static_assert(std::is_same_v<decltype(observable.subscribe(rpp::composite_disposable_wrapper::empty(), mock.get_observer().as_dynamic())), rpp::composite_disposable_wrapper>);
        auto d = observable.subscribe(rpp::composite_disposable_wrapper::empty(), mock.get_observer().as_dynamic());
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values().empty());
    }

    SUBCASE("subscribe lambdas")
    {
        static_assert(std::is_same_v<decltype(observable.subscribe(rpp::utils::empty_function_t<int>{}, rpp::utils::empty_function_t<std::exception_ptr>{}, rpp::utils::empty_function_t<>{})), void>);
        observable.subscribe([&mock](const auto& v) { mock.on_next(v); }, rpp::utils::empty_function_t<std::exception_ptr>{}, rpp::utils::empty_function_t<>{});
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe lambdas with disposable")
    {
        static_assert(std::is_same_v<decltype(observable.subscribe(rpp::composite_disposable_wrapper::make(), rpp::utils::empty_function_t<int>{}, rpp::utils::empty_function_t<std::exception_ptr>{}, rpp::utils::empty_function_t<>{})), rpp::composite_disposable_wrapper>);
        auto d = observable.subscribe(
            rpp::composite_disposable_wrapper::make(),
            [&mock](const auto& v) { mock.on_next(v); },
            rpp::utils::empty_function_t<std::exception_ptr>{},
            rpp::utils::empty_function_t<>{});
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values() == std::vector{1});
    }

    SUBCASE("subscribe lambdas with disposed disposable")
    {
        static_assert(std::is_same_v<decltype(observable.subscribe(rpp::composite_disposable_wrapper::empty(), rpp::utils::empty_function_t<int>{}, rpp::utils::empty_function_t<std::exception_ptr>{}, rpp::utils::empty_function_t<>{})), rpp::composite_disposable_wrapper>);
        auto d = observable.subscribe(
            rpp::composite_disposable_wrapper::empty(),
            [&mock](const auto& v) { mock.on_next(v); },
            rpp::utils::empty_function_t<std::exception_ptr>{},
            rpp::utils::empty_function_t<>{});
        CHECK(d.is_disposed());
        CHECK(mock.get_received_values().empty());
    }
}
