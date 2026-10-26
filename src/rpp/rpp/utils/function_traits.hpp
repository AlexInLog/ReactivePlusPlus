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

#include <rpp/utils/tuple.hpp>

#include <type_traits>

namespace rpp::utils
{
    template<typename T, typename = void>
    struct is_not_template_callable_t : std::false_type
    {
    };

    template<typename T>
    struct is_not_template_callable_t<T, std::void_t<decltype(&T::operator())>> : std::true_type
    {
    };

    template<class T, class R, class... Args>
    struct is_not_template_callable_t<R (T::*)(Args...) const> : std::true_type
    {
    };

    template<class T, class R, class... Args>
    struct is_not_template_callable_t<R (T::*)(Args...)> : std::true_type
    {
    };

    template<class R, class... Args>
    struct is_not_template_callable_t<R (*)(Args...)> : std::true_type
    {
    };

    template<typename T>
    concept is_not_template_callable = is_not_template_callable_t<T>::value;

    // Lambda
    template<is_not_template_callable T>
    struct function_traits : function_traits<decltype(&T::operator())>
    {
    };

    // Operator of lambda
    template<class T, class R, class... Args>
    struct function_traits<R (T::*)(Args...) const> : function_traits<R (*)(Args...)>
    {
    };

    // Operator of lambda with mutable
    template<class T, class R, class... Args>
    struct function_traits<R (T::*)(Args...)> : function_traits<R (*)(Args...)>
    {
    };

    // Classical global function no args
    template<class R>
    struct function_traits<R (*)()>
    {
        using result = R;
    };

    // Classical global function
    template<class R, class... Args>
    struct function_traits<R (*)(Args...)>
    {
        using result    = R;
        using arguments = rpp::utils::tuple<std::decay_t<Args>...>;

        template<std::size_t I = 0>
            requires (sizeof...(Args) > I)
        using argument = typename arguments::template type_at_index_t<I>;
    };

    template<typename T, std::size_t I = 0>
    using decayed_function_argument_t = typename function_traits<T>::template argument<I>;

    template<typename Fn, typename... Args>
    using decayed_invoke_result_t = std::decay_t<std::invoke_result_t<Fn, Args...>>;

} // namespace rpp::utils
