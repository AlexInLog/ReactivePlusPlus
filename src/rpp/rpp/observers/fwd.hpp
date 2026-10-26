//                  ReactivePlusPlus library
//
//          Copyright Aleksey Loginov 2022 - present.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          https://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/AlexInLog/ReactivePlusPlus
//

#pragma once

#include <rpp/disposables/fwd.hpp>
#include <rpp/observers/details/fwd.hpp>

#include <rpp/utils/constraints.hpp>
#include <rpp/utils/function_traits.hpp>
#include <rpp/utils/functors.hpp>
#include <rpp/utils/utils.hpp>

#include <exception>

namespace rpp::constraint
{
    template<typename S>
    concept observer_strategy_base = requires(const S& const_strategy, S& strategy, const rpp::disposable_wrapper& disposable) {
        // const_strategy.on_next(v);
        // const_strategy.on_next(std::move(mv));
        const_strategy.on_error(std::exception_ptr{});
        const_strategy.on_completed();

        strategy.set_upstream(disposable);
        {
            strategy.is_disposed()
        } -> std::same_as<bool>;
    };

    /**
     * @concept observer_strategy
     * @brief Concept defines requirements for an user-defined observer strategy.
     *
     * @tparam S is Strategy
     * @tparam Type is type of value observer would obtain
     *
     * @details Strategy should be able to handle:
     * - on_next for both: const lvalue ref and rvalue ref of Type
     * - on_error(exception_ptr) for unsuccessful termination event
     * - on_completed() for successful termination event
     * - set_upstream(disposable) for custom disposables related logic. In most cases you should OR do nothing OR just forward disposable to downstream observer (and set preferred_disposables_mode to None) OR fully handle disposales related logic properly
     * - is_disposed() for extending custom disposables related logic with indicating current status.
     * - `static constexpr rpp::details::observers::disposables_mode preferred_disposables_mode` with preferred disposables logic for observer over this strategy
     *
     * @ingroup observers
     */
    template<typename S, typename Type>
    concept observer_strategy = observer_strategy_base<S> && requires(const S& const_strategy, const Type& v, Type& mv) {
        const_strategy.on_next(v);
        const_strategy.on_next(std::move(mv));

        // strategy has to provide it's preferred disposable mode: minimal level of disposable logic it could work with.
        // if observer_strategy fully controls disposable logic or just forwards disposable to downstream observer: rpp::details::observers::disposables_mode::None
        // if you not sure about this field - just use rpp::details::observers::disposables_mode::Auto
        { std::decay_t<S>::preferred_disposables_mode } -> rpp::constraint::decayed_same_as<rpp::details::observers::disposables_mode>; /* = rpp::details::observers::disposables_mode::Auto */
    };
} // namespace rpp::constraint

namespace rpp::details::observers
{
    template<rpp::constraint::decayed_type Type>
    class dynamic_strategy;

    template<rpp::constraint::decayed_type             Type,
             std::invocable<Type>                      OnNext,
             std::invocable<const std::exception_ptr&> OnError,
             std::invocable<>                          OnCompleted>
    struct lambda_strategy;

    template<rpp::constraint::observer_strategy_base S, rpp::details::observers::constraint::disposables_strategy DisposableStrategy>
    struct override_disposables_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::Auto;

        override_disposables_strategy() = delete;

        consteval static void on_next(const auto&) noexcept { }
        consteval static void on_error(const std::exception_ptr&) noexcept { }
        consteval static void on_completed() noexcept { }

        consteval static void set_upstream(const disposable_wrapper&) noexcept { }
        consteval static bool is_disposed() noexcept { return false; }
    };
} // namespace rpp::details::observers
namespace rpp
{
    template<constraint::decayed_type Type, constraint::observer_strategy<Type> Strategy>
    class observer;

    /*
     * @brief Same as rpp::observer, but with passed rpp::composite_disposable_wrapper to constructor instead (as a result, it's possible to dispose observer early outside)
     * @ingroup observers
     */
    template<constraint::decayed_type Type, constraint::observer_strategy<Type> Strategy>
    using observer_with_external_disposable = observer<Type, rpp::details::observers::override_disposables_strategy<Strategy, rpp::details::observers::deduce_optimal_disposables_strategy_t<rpp::details::observers::disposables_mode::External>>>;

    template<constraint::decayed_type Type>
    class dynamic_observer;

    /**
     * @brief Observer specialized with passed callbacks. Most easiesest way to construct observer "on the fly" via lambdas and etc.
     *
     * @tparam Type of value this observer can handle
     * @tparam OnNext is type of callback to handle on_next(const Type&) and on_next(Type&&)
     * @tparam OnError is type of callback to handle on_error(const std::exception_ptr&)
     * @tparam OnCompleted is type of callback to handle on_completed()
     *
     * @ingroup observers
     */
    template<constraint::decayed_type Type, std::invocable<Type> OnNext, std::invocable<const std::exception_ptr&> OnError, std::invocable<> OnCompleted>
    using lambda_observer = observer<Type, details::observers::lambda_strategy<Type, OnNext, OnError, OnCompleted>>;

    /*
     * @brief Same as rpp::lambda_observer, but with passed rpp::composite_disposable_wrapper to constructor instead (as a result, it's possible to dispose observer early outside)
     * @ingroup observers
     */
    template<constraint::decayed_type Type, std::invocable<Type> OnNext, std::invocable<const std::exception_ptr&> OnError, std::invocable<> OnCompleted>
    using lambda_observer_with_external_disposable = observer_with_external_disposable<Type, details::observers::lambda_strategy<Type, OnNext, OnError, OnCompleted>>;

    /**
     * @brief Constructs observer specialized with passed callbacks. Most easiesest way to construct observer "on the fly" via lambdas and etc.
     *
     * @tparam Type of value this observer can handle
     * @param on_next is callback to handle on_next(const Type&) and on_next(Type&&)
     * @param on_error is callback to handle on_error(const std::exception_ptr&)
     * @param on_completed is callback to handle on_completed()
     *
     * @ingroup observers
     */
    template<constraint::decayed_type                  Type,
             std::invocable<Type>                      OnNext,
             std::invocable<const std::exception_ptr&> OnError     = rpp::utils::rethrow_error_t,
             std::invocable<>                          OnCompleted = rpp::utils::empty_function_t<>>
    auto make_lambda_observer(OnNext&&      on_next,
                              OnError&&     on_error     = {},
                              OnCompleted&& on_completed = {}) -> lambda_observer<Type,
                                                                                  std::decay_t<OnNext>,
                                                                                  std::decay_t<OnError>,
                                                                                  std::decay_t<OnCompleted>>;

    /**
     * @brief Constructs observer specialized with passed callbacks. Most easiesest way to construct observer "on the fly" via lambdas and etc.
     *
     * @tparam Type of value this observer can handle
     * @param d is disposable to attach to resulting observer
     * @param on_next is callback to handle on_next(const Type&) and on_next(Type&&)
     * @param on_error is callback to handle on_error(const std::exception_ptr&)
     * @param on_completed is callback to handle on_completed()
     *
     * @ingroup observers
     */
    template<constraint::decayed_type                  Type,
             std::invocable<Type>                      OnNext,
             std::invocable<const std::exception_ptr&> OnError     = rpp::utils::rethrow_error_t,
             std::invocable<>                          OnCompleted = rpp::utils::empty_function_t<>>
    auto make_lambda_observer(const rpp::composite_disposable_wrapper& d,
                              OnNext&&                                 on_next,
                              OnError&&                                on_error     = {},
                              OnCompleted&&                            on_completed = {}) -> lambda_observer_with_external_disposable<Type,
                                                                                                           std::decay_t<OnNext>,
                                                                                                           std::decay_t<OnError>,
                                                                                                           std::decay_t<OnCompleted>>;

    /**
     * @brief Constructs observer specialized with passed callbacks. Most easiesest way to construct observer "on the fly" via lambdas and etc.
     *
     * @param on_next is callback to handle on_next(const Type&) and on_next(Type&&)
     * @param on_error is callback to handle on_error(const std::exception_ptr&)
     * @param on_completed is callback to handle on_completed()
     * @tparam Type of value this observer can handle (deduced from OnNext callback)
     *
     * @ingroup observers
     */
    template<typename OnNext,
             std::invocable<const std::exception_ptr&> OnError     = rpp::utils::rethrow_error_t,
             std::invocable<>                          OnCompleted = rpp::utils::empty_function_t<>,
             constraint::decayed_type                  Type        = rpp::utils::decayed_function_argument_t<OnNext>>
        requires std::invocable<OnNext, Type>
    auto make_lambda_observer(OnNext&&      on_next,
                              OnError&&     on_error     = {},
                              OnCompleted&& on_completed = {})
    {
        return make_lambda_observer<Type>(std::forward<OnNext>(on_next), std::forward<OnError>(on_error), std::forward<OnCompleted>(on_completed));
    }

    /**
     * @brief Constructs observer specialized with passed callbacks. Most easiesest way to construct observer "on the fly" via lambdas and etc.
     *
     * @param d is disposable to attach to resulting observer
     * @param on_next is callback to handle on_next(const Type&) and on_next(Type&&)
     * @param on_error is callback to handle on_error(const std::exception_ptr&)
     * @param on_completed is callback to handle on_completed()
     * @tparam Type of value this observer can handle (deduced from OnNext callback)
     *
     * @ingroup observers
     */

    template<typename OnNext,
             std::invocable<const std::exception_ptr&> OnError     = rpp::utils::rethrow_error_t,
             std::invocable<>                          OnCompleted = rpp::utils::empty_function_t<>,
             constraint::decayed_type                  Type        = rpp::utils::decayed_function_argument_t<OnNext>>
        requires std::invocable<OnNext, Type>
    auto make_lambda_observer(const rpp::composite_disposable_wrapper& d,
                              OnNext&&                                 on_next,
                              OnError&&                                on_error     = {},
                              OnCompleted&&                            on_completed = {})
    {
        return make_lambda_observer<Type>(d, std::forward<OnNext>(on_next), std::forward<OnError>(on_error), std::forward<OnCompleted>(on_completed));
    }
} // namespace rpp

namespace rpp::details::observers
{
    struct fake_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        static void on_next(const auto&) noexcept { }

        static void on_error(const std::exception_ptr&) noexcept { }

        static void on_completed() noexcept { }

        static void set_upstream(const disposable_wrapper&) noexcept { }

        static bool is_disposed() noexcept { return true; }
    };

    template<typename T>
    using fake_observer = rpp::observer<T, fake_strategy>;
} // namespace rpp::details::observers

namespace rpp::utils
{
    template<typename T>
    using extract_observer_type_t = typename rpp::utils::extract_base_type_params_t<T, rpp::observer>::template type_at_index_t<0>;
} // namespace rpp::utils

namespace rpp::constraint
{
    template<typename T>
    concept observer = rpp::utils::is_base_of_v<T, rpp::observer>;

    template<typename T, typename Type>
    concept observer_of_type = observer<std::decay_t<T>> && std::same_as<rpp::utils::extract_observer_type_t<std::decay_t<T>>, Type>;
} // namespace rpp::constraint
