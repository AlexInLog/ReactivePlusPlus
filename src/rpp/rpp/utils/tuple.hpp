//                  ReactivePlusPlus library
//
//          Copyright Aleksey Loginov 2023 - present.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          https://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/AlexInLog/ReactivePlusPlus
//

#pragma once

#include <rpp/defs.hpp>
#include <rpp/utils/constraints.hpp>

#include <cstddef>
#include <utility>

namespace rpp::details
{
    template<size_t, typename T>
    class tuple_leaf
    {
    public:
        tuple_leaf() = default;

        tuple_leaf(const T& value)
            : m_value{value}
        {
        }

        tuple_leaf(T&& value)
            : m_value{std::move(value)}
        {
        }

        const T& get() const { return m_value; }

        T& get() { return m_value; }

    private:
        RPP_NO_UNIQUE_ADDRESS T m_value{};
    };

    template<typename, typename...>
    class tuple_impl;

    template<typename... Args, size_t... Indices>
    class RPP_EMPTY_BASES tuple_impl<std::index_sequence<Indices...>, Args...> : private tuple_leaf<Indices, Args>...
    {
    public:
        tuple_impl() = default;

        template<typename... TArgs>
            requires (!rpp::constraint::variadic_decayed_same_as<tuple_impl<std::index_sequence<Indices...>, Args...>, TArgs...>)
        tuple_impl(TArgs&&... args)
            : tuple_leaf<Indices, Args>{std::forward<TArgs>(args)}...
        {
        }

        template<typename... TArgs, std::invocable<TArgs&&..., Args&...> Callable>
        auto apply(Callable&& callable, TArgs&&... args)
        {
            return std::forward<Callable>(callable)(std::forward<TArgs>(args)..., static_cast<tuple_leaf<Indices, Args>*>(this)->get()...);
        }

        template<typename... TArgs, std::invocable<TArgs&&..., Args...> Callable>
        auto apply(Callable&& callable, TArgs&&... args) const
        {
            return std::forward<Callable>(callable)(std::forward<TArgs>(args)..., static_cast<const tuple_leaf<Indices, Args>*>(this)->get()...);
        }

        template<size_t I>
            requires (I < sizeof...(Args))
        const auto& get() const
        {
            return static_cast<const tuple_leaf<I, type_at_index_t<I>>*>(this)->get();
        }

        template<size_t I>
            requires (I < sizeof...(Args))
        auto& get()
        {
            return static_cast<tuple_leaf<I, type_at_index_t<I>>*>(this)->get();
        }

    private:
        template<size_t I, typename T>
        constexpr static T type_at_index_impl(const tuple_leaf<I, T>*);

    public:
        template<size_t I>
        constexpr static auto type_at_index() -> decltype(type_at_index_impl<I>(std::declval<tuple_impl*>()));

        template<size_t I>
            requires (I < sizeof...(Args))
        using type_at_index_t = decltype(type_at_index<I>());
    };
} // namespace rpp::details

namespace rpp::utils
{
    template<typename... Args>
    class tuple : public rpp::details::tuple_impl<std::index_sequence_for<Args...>, Args...>
    {
    public:
        using rpp::details::tuple_impl<std::index_sequence_for<Args...>, Args...>::tuple_impl;
    };

    template<typename... Args>
    tuple(const Args&...) -> tuple<Args...>;
} // namespace rpp::utils
