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

#include <rpp/defs.hpp>
#include <rpp/observables/observable.hpp>

namespace rpp::details
{
    template<typename Factory>
    struct defer_strategy
    {
        using value_type                   = rpp::utils::extract_observable_type_t<std::invoke_result_t<Factory>>;
        using optimal_disposables_strategy = typename std::invoke_result_t<Factory>::optimal_disposables_strategy;

        RPP_NO_UNIQUE_ADDRESS Factory observable_factory;

        template<rpp::constraint::observer_of_type<value_type> TObs>
        void subscribe(TObs&& obs) const
        {
            observable_factory().subscribe(std::forward<TObs>(obs));
        }
    };
} // namespace rpp::details

namespace rpp
{
    template<constraint::decayed_type Type, typename Factory>
    using defer_observable = observable<Type, details::defer_strategy<Factory>>;
} // namespace rpp

namespace rpp::source
{

    /**
     * @brief Creates rpp::observable that calls the specified observable factory to create an observable for each new observer that subscribes.
     *
     * @param observable_factory is function to create observable to subscribe on.
     *
     * @par Example:
     * @snippet defer.cpp defer from_iterable
     *
     * @ingroup creational_operators
     * @see https://reactivex.io/documentation/operators/defer.html
     */
    template<std::invocable Factory>
        requires rpp::constraint::observable<std::invoke_result_t<Factory>>
    auto defer(Factory&& observable_factory)
    {
        return defer_observable<rpp::utils::extract_observable_type_t<std::invoke_result_t<Factory>>, Factory>{std::forward<Factory>(observable_factory)};
    }
} // namespace rpp::source
