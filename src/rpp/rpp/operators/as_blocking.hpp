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

#include <rpp/observables/fwd.hpp>
#include <rpp/operators/fwd.hpp>

#include <rpp/observables/blocking_observable.hpp>

namespace rpp::operators::details
{
    struct as_blocking_t
    {
        template<rpp::constraint::decayed_type Type, rpp::constraint::observable_strategy<Type> Strategy>
        auto operator()(rpp::observable<Type, Strategy>&& observable) const
        {
            return rpp::blocking_observable<Type, Strategy>{std::move(observable)};
        }

        template<rpp::constraint::decayed_type Type, rpp::constraint::observable_strategy<Type> Strategy>
        auto operator()(const rpp::observable<Type, Strategy>& observable) const
        {
            return rpp::blocking_observable<Type, Strategy>{observable};
        }
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Converts `rpp::observable` to `rpp::blocking_observable`
     * @details `rpp::blocking_observable` blocks `subscribe` call till on_completed/on_error happens.
     *
     * @par Example:
     * @snippet as_blocking.cpp as_blocking
     *
     * @ingroup utility_operators
     */
    inline auto as_blocking()
    {
        return details::as_blocking_t{};
    }
} // namespace rpp::operators
