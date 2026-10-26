//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/sources/fwd.hpp>

#include <rpp/observables/observable.hpp>

namespace rpp::details
{
    template<constraint::decayed_type Type>
    struct never_strategy
    {
        using value_type                   = Type;
        using optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<0>;

        static void subscribe(const auto&) { }
    };
} // namespace rpp::details

namespace rpp
{
    template<constraint::decayed_type Type>
    using never_observable = observable<Type, details::never_strategy<Type>>;
} // namespace rpp

namespace rpp::source
{
    /**
     * @brief Creates rpp::observable that emits no items and does not terminate
     *
     * @marble never
       {
           operator "never": +>
       }
     * @tparam Type type of value to specify observable
     *
     * @ingroup creational_operators
     * @see https://reactivex.io/documentation/operators/empty-never-throw.html
     */
    template<constraint::decayed_type Type>
    auto never()
    {
        return never_observable<Type>{};
    }
} // namespace rpp::source
