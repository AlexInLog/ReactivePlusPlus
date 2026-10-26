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

#include <rpp/operators/fwd.hpp>

#include <rpp/sources/concat.hpp>
#include <rpp/utils/utils.hpp>

#include <cstddef>

namespace rpp::operators::details
{
    struct repeat_t
    {
        size_t count;

        template<rpp::constraint::observable TObservable>
        auto operator()(TObservable&& observable) const
        {
            return rpp::source::concat(utils::repeated_container{std::forward<TObservable>(observable), count});
        }
    };

    struct infinite_repeat_t
    {
        template<rpp::constraint::observable TObservable>
        auto operator()(TObservable&& observable) const
        {
            return rpp::source::concat(utils::infinite_repeated_container{std::forward<TObservable>(observable)});
        }
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Repeats the Observabe's sequence of emissions `count` times via re-subscribing on it during `on_completed` call while `count` not reached.
     *
     * @marble repeat
         {
             source observable    : +-1-2-3-|
             operator "repeat(2)" : +-1-2-3-1-2-3-|
         }
     *
     * @details Actually this operator is kind of `concat(obs, obs...)` where obs repeated `count` times
     *
     * @param count total amount of times subscription happens. For example:
     *  - `repeat(0)`  - means no any subscription at all
     *  - `repeat(1)`  - behave like ordinal observable
     *  - `repeat(10)` - 1 normal subscription and 9 re-subscriptions during `on_completed`
     *
     * @note `#include <rpp/operators/repeat.hpp>`
     *
     * @par Examples:
     * @snippet repeat.cpp repeat
     *
     * @ingroup utility_operators
     * @see https://reactivex.io/documentation/operators/repeat.html
     */

    inline auto repeat(size_t count)
    {
        return details::repeat_t{count};
    }

    /**
     * @brief Repeats the Observabe's sequence of emissions infinite amount of times via re-subscribing on it during `on_completed`.
     *
     * @marble repeat_infinitely
         {
             source observable : +-1-2-3-|
             operator "repeat" : +-1-2-3-1-2-3-1-2-3>
         }
     *
     * @details Actually this operator is kind of `concat(obs, obs...)`
     *
     * @note `#include <rpp/operators/repeat.hpp>`
     *
     * @par Examples:
     * @snippet repeat.cpp repeat_infinitely
     *
     * @ingroup utility_operators
     * @see https://reactivex.io/documentation/operators/repeat.html
     */
    inline auto repeat()
    {
        return details::infinite_repeat_t{};
    }
} // namespace rpp::operators
