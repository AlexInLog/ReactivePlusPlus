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

#include <concepts>
#include <iterator>

namespace rpp::constraint
{
    template<typename T, typename Type>
    concept decayed_same_as = std::same_as<std::decay_t<T>, std::decay_t<Type>>;

    template<typename T>
    concept decayed_type = std::same_as<std::decay_t<T>, T>;

    template<typename T, typename... Types>
    concept any_of = (std::same_as<T, Types> || ...);

    template<typename T, typename... Types>
    concept decayed_any_of = (decayed_same_as<T, Types> || ...);

    template<typename Type, typename... Types>
    concept variadic_decayed_same_as = sizeof...(Types) == 1 && (decayed_same_as<Type, Types> && ...);

    template<typename T, typename Target>
    concept static_pointer_convertible_to = requires(T* ptr) { static_cast<Target*>(ptr); };

    template<typename R>
    concept iterable = requires(R& rng) {
        std::cbegin(rng);
        std::cend(rng);
    };

    template<typename T, typename... Args>
    concept is_constructible_from = requires(Args... args) {
        {
            T{static_cast<Args&&>(args)...}
        } -> std::same_as<T>;
    };

    template<typename Ret, typename Fn, typename... Args>
    concept invocable_r_v = std::invocable<Fn, Args...> && std::same_as<Ret, std::invoke_result_t<Fn, Args...>>;

    template<typename Fn, typename... Args>
    concept is_nothrow_invocable = std::is_nothrow_invocable_v<Fn, Args...>;

    template<typename T>
    concept hashable = requires(T a) {
        {
            std::hash<T>{}(a)
        } -> std::convertible_to<std::size_t>;
    };
} // namespace rpp::constraint
