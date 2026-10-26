//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/observables/fwd.hpp>
#include <rpp/observers/fwd.hpp>

#include <rpp/defs.hpp>
#include <rpp/schedulers/current_thread.hpp>

namespace rpp::details::observables
{
    template<typename TStrategy, typename... TStrategies>
    class chain
    {
        using base = chain<TStrategies...>;

        using operator_traits = typename TStrategy::template operator_traits<typename base::value_type>;

        static_assert(rpp::constraint::operator_<TStrategy, typename base::value_type, typename base::optimal_disposables_strategy>);

    public:
        using optimal_disposables_strategy = typename TStrategy::template updated_optimal_disposables_strategy<typename base::optimal_disposables_strategy>;
        using value_type                   = typename operator_traits::result_type;

        chain(const TStrategy& strategy, const TStrategies&... strategies)
            : m_strategy(strategy)
            , m_strategies(strategies...)
        {
        }

        chain(const TStrategy& strategy, const chain<TStrategies...>& strategies)
            : m_strategy(strategy)
            , m_strategies(strategies)
        {
        }

        template<rpp::constraint::observer_of_type<value_type> Observer>
        void subscribe(Observer&& observer) const
        {
            [[maybe_unused]] const auto drain_on_exit = own_current_thread_if_needed();

            if constexpr (rpp::constraint::operator_lift_with_disposables_strategy<TStrategy, typename base::value_type, typename base::optimal_disposables_strategy>)
                m_strategies.subscribe(m_strategy.template lift_with_disposables_strategy<typename base::value_type, typename base::optimal_disposables_strategy>(std::forward<Observer>(observer)));
            else if constexpr (rpp::constraint::operator_lift<TStrategy, typename base::value_type>)
                m_strategies.subscribe(m_strategy.template lift<typename base::value_type>(std::forward<Observer>(observer)));
            else
            {
                static_assert(rpp::constraint::operator_subscribe<TStrategy, typename base::value_type>);
                m_strategy.subscribe(std::forward<Observer>(observer), m_strategies);
            }
        }

    private:
        static auto own_current_thread_if_needed()
        {
            if constexpr (requires { requires operator_traits::own_current_queue; })
                return rpp::schedulers::current_thread::own_queue_and_drain_finally_if_not_owned();
            else
                return rpp::utils::none{};
        }

    private:
        RPP_NO_UNIQUE_ADDRESS TStrategy             m_strategy;
        RPP_NO_UNIQUE_ADDRESS chain<TStrategies...> m_strategies;
    };

    template<typename TStrategy>
    class chain<TStrategy>
    {
    public:
        using optimal_disposables_strategy = typename TStrategy::optimal_disposables_strategy;
        using value_type                   = typename TStrategy::value_type;

        chain(const TStrategy& strategy)
            : m_strategy(strategy)
        {
        }

        template<rpp::constraint::observer Observer>
        void subscribe(Observer&& observer) const
        {
            m_strategy.subscribe(std::forward<Observer>(observer));
        }

    private:
        RPP_NO_UNIQUE_ADDRESS TStrategy m_strategy;
    };

    template<typename New, typename Old>
    struct make_chain
    {
        using type = chain<New, Old>;
    };

    template<typename New, typename... Args>
    struct make_chain<New, chain<Args...>>
    {
        using type = chain<New, Args...>;
    };

    template<typename New, typename Old>
    using make_chain_t = typename make_chain<New, Old>::type;
} // namespace rpp::details::observables
