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
    struct error_strategy
    {
        using value_type                   = Type;
        using optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<0>;

        std::exception_ptr err{};

        void subscribe(const auto& obs) const { obs.on_error(err); }
    };
} // namespace rpp::details

namespace rpp
{
    template<constraint::decayed_type Type>
    using error_observable = observable<Type, details::error_strategy<Type>>;
} // namespace rpp

namespace rpp::source
{
    /**
     * @brief Creates rpp::observable that emits no items and terminates with an error
     *
     * @marble error
      {
          operator "error": +#
      }
     * @tparam Type type of value to specify observable
     * @param err exception ptr to be sent to subscriber
     *
     * @ingroup creational_operators
     * @see https://reactivex.io/documentation/operators/empty-never-throw.html
     */
    template<constraint::decayed_type Type>
    auto error(std::exception_ptr err)
    {
        return error_observable<Type>{std::move(err)};
    }
} // namespace rpp::source
