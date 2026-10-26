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

    struct empty_strategy
    {
        using value_type                   = Type;
        using optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<0>;

        static void subscribe(const auto& obs) { obs.on_completed(); }
    };
} // namespace rpp::details

namespace rpp
{
    template<constraint::decayed_type Type>
    using empty_observable = observable<Type, details::empty_strategy<Type>>;
} // namespace rpp

namespace rpp::source
{
    /**
     * @brief Creates rpp::observable that emits no items but terminates normally
     *
     * @marble empty
       {
           operator "empty": +|
       }
     *
     * @tparam Type type of value to specify observable
     *
     * @ingroup creational_operators
     * @see https://reactivex.io/documentation/operators/empty-never-throw.html
     */
    template<constraint::decayed_type Type>
    auto empty()
    {
        return empty_observable<Type>{};
    }
} // namespace rpp::source
