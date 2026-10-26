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

#include <rpp/observers/fwd.hpp>
#include <rpp/sources/fwd.hpp>

#include <rpp/defs.hpp>
#include <rpp/disposables/disposable_wrapper.hpp>
#include <rpp/observables/details/chain_strategy.hpp>
#include <rpp/observables/observable.hpp>
#include <rpp/utils/constraints.hpp>
#include <rpp/utils/tuple.hpp>
#include <rpp/utils/utils.hpp>

namespace rpp::operators::details
{
    template<typename Operator, rpp::constraint::decayed_type... TArgs>
    class lift_operator
    {
    public:
        template<rpp::constraint::decayed_same_as<TArgs>... TTArgs>
        lift_operator(TTArgs&&... args)
            : m_vals{std::forward<TTArgs>(args)...}
        {
        }

        template<rpp::constraint::decayed_type Type, rpp::constraint::observer Observer>
        auto lift(Observer&& observer) const
        {
            return m_vals.apply(&apply<Type, Observer, TArgs...>, std::forward<Observer>(observer));
        }

    private:
        template<rpp::constraint::decayed_type Type,
                 rpp::constraint::observer     Observer,
                 typename... Args>
        static auto apply(Observer&& observer, const Args&... vals)
        {
            static_assert(rpp::constraint::observer_of_type<std::decay_t<Observer>, typename Operator::template operator_traits<Type>::result_type>);
            return rpp::observer<Type, typename Operator::template operator_traits<Type>::template observer_strategy<std::decay_t<Observer>>>{std::forward<Observer>(observer), vals...}; // NOLINT
        }

    private:
        RPP_NO_UNIQUE_ADDRESS rpp::utils::tuple<TArgs...> m_vals{};
    };
} // namespace rpp::operators::details
