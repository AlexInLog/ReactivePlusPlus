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

#include <rpp/observables/connectable_observable.hpp>

namespace rpp::operators::details
{
    template<rpp::constraint::subject Subject>
    struct multicast_t
    {
        RPP_NO_UNIQUE_ADDRESS Subject m_subject;

        template<rpp::constraint::observable TObservable>
        auto operator()(TObservable&& observable) const
        {
            static_assert(std::same_as<rpp::utils::extract_observable_type_t<TObservable>, rpp::subjects::utils::extract_subject_type_t<Subject>>, "observable and subject should be of same type");
            return rpp::connectable_observable<std::decay_t<TObservable>, Subject>{std::forward<TObservable>(observable), m_subject};
        }
    };

    template<template<typename> typename Subject>
    struct template_multicast_t
    {
        template<rpp::constraint::observable TObservable>
        auto operator()(TObservable&& observable) const
        {
            static_assert(rpp::constraint::subject<Subject<rpp::utils::extract_observable_type_t<TObservable>>>, "subject should be constructible with type of observable");

            return rpp::connectable_observable<std::decay_t<TObservable>,
                                               Subject<rpp::utils::extract_observable_type_t<TObservable>>>{std::forward<TObservable>(observable),
                                                                                                            Subject<rpp::utils::extract_observable_type_t<TObservable>>{}};
        }
    };

} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Converts ordinary observable to rpp::connectable_observable with help of provided subject
     * @details Connectable observable is common observable, but actually it starts emissions of items only after call "connect", "ref_count" or any other available way. Also it uses subject to multicast values to subscribers
     * @warning Same subject would be used to all observables lifted via this operator. To have fresh subject everytime use another overloading
     *
     * @param subject is subject used to create rpp::connectable_observable
     * @note `#include <rpp/operators/multicast.hpp>`
     *
     * @par Example
     * @snippet multicast.cpp multicast
     *
     * @ingroup connectable_operators
     * @see https://reactivex.io/documentation/operators/publish.html
     */
    template<rpp::constraint::subject Subject>
    auto multicast(Subject&& subject)
    {
        return details::multicast_t<std::decay_t<Subject>>{std::forward<Subject>(subject)};
    }

    /**
     * @brief Converts ordinary observable to rpp::connectable_observable with help of inline instsantiated subject of provided type
     * @details Connectable observable is common observable, but actually it starts emissions of items only after call "connect", "ref_count" or any other available way. Also it uses subject to multicast values to subscribers
     * @warning This overloading creates fresh `Subject<Type>` everytime new observable passed to this operator
     *
     * @tparam Subject is template teamplate typename over Subject to be created to create corresponding connectable_observable for provided observable
     * @note `#include <rpp/operators/multicast.hpp>`
     *
     * @par Example
     * @snippet multicast.cpp multicast_template
     *
     * @ingroup connectable_operators
     * @see https://reactivex.io/documentation/operators/publish.html
     */
    template<template<typename> typename Subject>
    auto multicast()
    {
        return details::template_multicast_t<Subject>{};
    }
} // namespace rpp::operators
