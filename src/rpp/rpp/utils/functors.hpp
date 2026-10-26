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

#include <exception>
#include <tuple>

namespace rpp::utils
{
    template<class... Ts>
    struct overloaded : Ts...
    {
        using Ts::operator()...;
    };
    template<class... Ts>
    overloaded(Ts...) -> overloaded<Ts...>;

    template<typename... Types>
    struct empty_function_t
    {
        constexpr void operator()(const Types&...) const noexcept { }
    };

    template<typename... Types>
    void empty_function(const Types&...)
    {
    }

    struct empty_function_any_t
    {
        template<typename... Types>
        constexpr void operator()(const Types&...) const noexcept
        {
        }
    };

    struct empty_function_any_by_lvalue_t
    {
        template<typename... Types>
        constexpr void operator()(Types...) const noexcept
        {
        }
    };

    struct rethrow_error_t
    {
        [[noreturn]] void operator()(const std::exception_ptr& err) const noexcept { std::rethrow_exception(err); }
    };

    struct equal_to
    {
        template<typename T>
        bool operator()(const T& l, const T& r) const
        {
            return l == r;
        }
    };

    struct return_true
    {
        bool operator()() const { return true; }
    };

    struct less
    {
        template<typename T>
        bool operator()(const T& l, const T& r) const
        {
            return l < r;
        }
    };

    struct pack_to_tuple
    {
        auto operator()(auto&&... vals) const { return std::make_tuple(std::forward<decltype(vals)>(vals)...); }
    };
} // namespace rpp::utils
