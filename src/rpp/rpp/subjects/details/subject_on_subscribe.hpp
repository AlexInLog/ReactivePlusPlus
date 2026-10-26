//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2022 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/sources/fwd.hpp>

#include <rpp/observables/observable.hpp>

namespace rpp::subjects::details
{
    template<constraint::decayed_type Type, ::rpp::constraint::on_subscribe<Type> OnSubscribe, typename DisposableStrategy>
    struct subject_on_subscribe_strategy
    {
        using value_type                   = Type;
        using optimal_disposables_strategy = DisposableStrategy;

        RPP_NO_UNIQUE_ADDRESS OnSubscribe subscribe;
    };

    template<constraint::decayed_type Type, typename DisposableStrategy, rpp::constraint::on_subscribe<Type> OnSubscribe>
    auto create_subject_on_subscribe_observable(OnSubscribe&& on_subscribe)
    {
        return rpp::observable<Type, subject_on_subscribe_strategy<Type, std::decay_t<OnSubscribe>, DisposableStrategy>>(std::forward<OnSubscribe>(on_subscribe));
    }
} // namespace rpp::subjects::details
