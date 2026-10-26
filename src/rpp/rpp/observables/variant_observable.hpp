//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/observables/observable.hpp>
#include <rpp/utils/utils.hpp>

#include <variant>

namespace rpp::details
{
    template<constraint::decayed_type Type, constraint::observable_of_type<Type>... Observables>
    struct variant_observable_strategy
    {
        using optimal_disposables_strategy = rpp::details::observables::default_disposables_strategy;

        using value_type = Type;
        template<constraint::decayed_any_of<Observables...> TT>
            requires (!constraint::decayed_same_as<variant_observable_strategy, TT>)
        explicit variant_observable_strategy(TT&& observable) // NOLINT
            : observables(std::forward<TT>(observable))
        {
        }

        variant_observable_strategy(const variant_observable_strategy& other)     = default;
        variant_observable_strategy(variant_observable_strategy&& other) noexcept = default;

        utils::unique_variant<Observables...> observables;

        template<rpp::constraint::observer_of_type<value_type> TObs>
        void subscribe(TObs&& obs) const
        {
            std::visit([&](const auto& o) { o.subscribe(std::forward<TObs>(obs)); }, observables);
        }
    };
} // namespace rpp::details

namespace rpp
{
    /**
     * @brief Extension over rpp::observable to provide ability statically keep one of multiple observables
     *
     * @ingroup observables
     */
    template<constraint::decayed_type Type, constraint::observable_of_type<Type>... Observables>
    class variant_observable : public rpp::observable<Type, details::variant_observable_strategy<Type, Observables...>>
    {
        using base = rpp::observable<Type, details::variant_observable_strategy<Type, Observables...>>;

    public:
        using base::base;
    };

    template<constraint::observable T, constraint::observable_of_type<rpp::utils::extract_observable_type_t<T>>... Observables>
    variant_observable(std::variant<T, Observables...> variant) -> variant_observable<rpp::utils::extract_observable_type_t<T>, T, Observables...>;
} // namespace rpp
