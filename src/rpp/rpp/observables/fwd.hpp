//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/observers/fwd.hpp>

#include <rpp/observables/details/disposables_strategy.hpp>
#include <rpp/utils/constraints.hpp>
#include <rpp/utils/utils.hpp>

namespace rpp::constraint
{
    /**
     * @concept observable_strategy
     * @brief A concept that defines the requirements for an observable strategy.
     *
     * This concept ensures that a type `S` meets the following criteria:
     * - It has a `subscribe` method that accepts observer of type `T` and returns `void`.
     * - It defines a nested type `value_type` to represent the type of values emitted by the observable.
     * - It defines a nested type `optimal_disposables_strategy` to define the optimal disposables strategy observer could/should use to handle current observable properly.
     *
     * @tparam S The type to be checked against the concept.
     * @tparam T The type of the values emitted by the observable.
     *
     * @ingroup observables
     */
    template<typename S, typename T>
    concept observable_strategy = requires(const S& strategy, rpp::details::observers::fake_observer<T>&& observer) {
        {
            strategy.subscribe(std::move(observer))
        } -> std::same_as<void>;

        typename S::value_type;
        typename S::optimal_disposables_strategy;
        requires rpp::details::observables::constraint::disposables_strategy<typename S::optimal_disposables_strategy>;
    };
} // namespace rpp::constraint

namespace rpp::details::observables
{
    template<rpp::constraint::decayed_type Type>
    class dynamic_strategy;

    template<rpp::constraint::decayed_type Type, rpp::constraint::observable_strategy<Type> Strategy>
    class blocking_strategy;

    template<rpp::constraint::decayed_type Type>
    struct fake_strategy
    {
        using value_type                   = Type;
        using optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<0>;

        consteval static void subscribe(const auto&) { }
    };
} // namespace rpp::details::observables

namespace rpp::details::observables
{
    template<typename TStrategy, typename... TStrategies>
    class chain;
} // namespace rpp::details::observables

namespace rpp
{
    template<rpp::constraint::decayed_type Type, rpp::constraint::observable_strategy<Type> Strategy>
    class observable;
} // namespace rpp


namespace rpp::constraint
{
    template<typename T>
    concept observable = rpp::utils::is_base_of_v<T, rpp::observable>;
} // namespace rpp::constraint

namespace rpp
{
    template<rpp::constraint::observable OriginalObservable, typename Subject>
    class connectable_observable;
} // namespace rpp

namespace rpp::utils
{
    template<typename T>
    using extract_observable_type_t = typename rpp::utils::extract_base_type_params_t<T, rpp::observable>::template type_at_index_t<0>;
} // namespace rpp::utils

namespace rpp::constraint
{
    template<typename T, typename Type>
    concept observable_of_type = observable<T> && std::same_as<utils::extract_observable_type_t<T>, std::decay_t<Type>>;

    template<typename TObservable, typename... TObservables>
    concept observables_of_same_type = rpp::constraint::observable<TObservable> && (rpp::constraint::observable<TObservables> && ...) && (std::same_as<rpp::utils::extract_observable_type_t<TObservable>, rpp::utils::extract_observable_type_t<TObservables>> && ...);

    /**
     * @concept operator_subscribe
     * @brief Simple operator defining logic how to subscribe passed observer to passed observable. In most cases it means operator have some custom logic over observable too, so, you need to have access to observable, for example, subscribe to observable multiple times.
     * @ingroup operators
     */
    template<typename Op, typename Type>
    concept operator_subscribe = requires(const Op& op, rpp::details::observers::fake_observer<typename std::decay_t<Op>::template operator_traits<Type>::result_type>&& observer, const details::observables::chain<details::observables::fake_strategy<Type>>& chain) {
        {
            op.subscribe(std::move(observer), chain)
        };
    };

    /**
     * @concept operator_lift
     * @brief Accept downstream observer and return new upstream (of type Type) observer.
     * @ingroup operators
     */
    template<typename Op, typename Type>
    concept operator_lift = requires(const Op& op, rpp::details::observers::fake_observer<typename std::decay_t<Op>::template operator_traits<Type>::result_type>&& observer) {
        {
            op.template lift<Type>(std::move(observer))
        } -> rpp::constraint::observer_of_type<Type>;
    };

    /**
     * @concept operator_lift_with_disposables_strategy
     * @brief Same as @link rpp::constraint::operator_lift @endlink but with custom disposables logic. For example, if you are manually create storage for disposables and want to do it optimal.
     * @ingroup operators
     */

    template<typename Op, typename Type, typename DisposableStrategy>
    concept operator_lift_with_disposables_strategy = requires(const Op& op, rpp::details::observers::fake_observer<typename std::decay_t<Op>::template operator_traits<Type>::result_type>&& observer) {
        {
            op.template lift_with_disposables_strategy<Type, DisposableStrategy>(std::move(observer))
        } -> rpp::constraint::observer_of_type<Type>;
    };

    template<typename Op, typename Type>
    concept has_operator_traits = requires() {
        typename std::decay_t<Op>::template operator_traits<Type>;
        typename std::decay_t<Op>::template operator_traits<Type>::result_type;
    };

    template<typename Op>
    concept has_operator_disposables_strategy = requires() {
        typename std::decay_t<Op>::template updated_optimal_disposables_strategy<typename details::observables::default_disposables_strategy>;
    } && details::observables::constraint::disposables_strategy<typename std::decay_t<Op>::template updated_optimal_disposables_strategy<typename details::observables::default_disposables_strategy>>;

    /**
     * @concept operator_
     * @details Concept for any RPP-related operator:
     * - operator should have type-traits: template sub-struct `operator_traits` where template typename is type of upstream.
     *   - Such and sub-struct should have `result_type` using to type of final observable/downstream observer
     *   - You can place any static_asserts to this `operator_traits` if you have some specific requiremenets
     * - operator should have template using `updated_optimal_disposables_strategy` accepting `rpp::details::observables::disposbles_strategy` and returning new (updated) strategy to provide optimal behavior. For example, your operator could add+1 disposables to the strategy
     * - operator should satisfy `rpp::constraint::operator_subscribe`, `rpp::constraint::operator_lift` or `rpp::constraint::operator_lift_with_disposables_strategy`
     *
     * @ingroup operators
     */
    template<typename Op, typename Type, typename DisposableStrategy>
    concept operator_ =
        has_operator_traits<Op, Type>
        && has_operator_disposables_strategy<Op>
        && (operator_subscribe<std::decay_t<Op>, Type> || operator_lift<std::decay_t<Op>, Type> || operator_lift_with_disposables_strategy<std::decay_t<Op>, Type, DisposableStrategy>);

} // namespace rpp::constraint

namespace rpp
{
    template<constraint::decayed_type Type>
    class dynamic_observable;

    template<typename Subject>
    class dynamic_connectable_observable;

    template<constraint::decayed_type Type, constraint::observable_strategy<Type> Strategy>
    class blocking_observable;

    template<constraint::decayed_type KeyType, constraint::decayed_type Type, constraint::observable_strategy<Type> Strategy>
    class grouped_observable;

    template<constraint::decayed_type Type, rpp::constraint::observable_of_type<Type>... Observables>
    class variant_observable;
} // namespace rpp
