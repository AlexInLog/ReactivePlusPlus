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

#include <rpp/defs.hpp>
#include <rpp/operators/details/strategy.hpp>

namespace rpp::operators::details
{
    template<rpp::constraint::decayed_type LastFn>
    struct finally_t
    {
        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            using result_type = T;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = typename Prev::template add<1>;

        RPP_NO_UNIQUE_ADDRESS LastFn last_fn;

        template<rpp::constraint::decayed_type Type, rpp::constraint::observer Observer>
        auto lift(Observer&& observer) const
        {
            observer.set_upstream(make_callback_disposable(last_fn));
            return std::forward<Observer>(observer);
        }
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Register callback to be called when execution is done and disposable bound to observer is disposed
     *
     * @param last_fn action callback
     * @note `#include <rpp/operators/finally.hpp>`
     *
     * @details action callback needs to be noexcept as it is called on dispose, throwing during this time could potentially break internal disposable state.
     *
     * @ingroup utility_operators
     * @see https://reactivex.io/documentation/operators/do.html
     */
    template<rpp::constraint::is_nothrow_invocable LastFn>
    auto finally(LastFn&& last_fn)
    {
        return details::finally_t<std::decay_t<LastFn>>{std::forward<LastFn>(last_fn)};
    }
} // namespace rpp::operators
