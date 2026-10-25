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

#include <rpp/operators/multicast.hpp>
#include <rpp/subjects/publish_subject.hpp>

namespace rpp::operators::details
{
    template<template<typename> typename Subject>
    struct share_t
    {
        template<rpp::constraint::observable TObservable>
        auto operator()(TObservable&& observable) const
        {
            return template_multicast_t<Subject>{}(std::forward<TObservable>(observable)).ref_count();
        }
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Shares single subscription to original observable between multiple observers
     * @details This is a shortcut for `multicast<Subject>() | ref_count()`: observable is converted to rpp::connectable_observable with help of inline instantiated subject and immediately forced to behave like common observable. As a result original observable is subscribed on the first subscription and disposed on the last unsubscription, while emissions are multicasted to all observers.
     *
     * @warning This operator creates fresh `Subject<Type>` everytime new observable passed to it, but same subject is reused for all observers of that particular observable. In case of rpp::subjects::publish_subject observers subscribed after some emissions would obtain only upcoming ones
     *
     * @tparam Subject is template template typename over Subject to be created to multicast emissions, rpp::subjects::publish_subject by default
     * @note `#include <rpp/operators/share.hpp>`
     *
     * @par Example
     * @snippet share.cpp share
     *
     * @ingroup connectable_operators
     * @see https://reactivex.io/documentation/operators/refcount.html
     */
    template<template<typename> typename Subject>
    auto share()
    {
        return details::share_t<Subject>{};
    }
} // namespace rpp::operators
