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

#include <rpp/operators/fwd.hpp>

#include <rpp/defs.hpp>
#include <rpp/operators/details/strategy.hpp>

namespace rpp::operators::details
{
    template<
        rpp::constraint::observer     TObserver,
        rpp::constraint::decayed_type OnNext,
        rpp::constraint::decayed_type OnError,
        rpp::constraint::decayed_type OnCompleted>
    struct tap_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        RPP_NO_UNIQUE_ADDRESS TObserver   observer;
        RPP_NO_UNIQUE_ADDRESS OnNext      onNext;
        RPP_NO_UNIQUE_ADDRESS OnError     onError;
        RPP_NO_UNIQUE_ADDRESS OnCompleted onCompleted;

        template<typename T>
        void on_next(T&& v) const
        {
            onNext(utils::as_const(v));
            observer.on_next(std::forward<T>(v));
        }

        void on_error(const std::exception_ptr& err) const
        {
            onError(err);
            observer.on_error(err);
        }

        void on_completed() const
        {
            onCompleted();
            observer.on_completed();
        }

        void set_upstream(const disposable_wrapper& d) { observer.set_upstream(d); }

        bool is_disposed() const { return observer.is_disposed(); }
    };

    template<
        rpp::constraint::decayed_type OnNext,
        rpp::constraint::decayed_type OnError,
        rpp::constraint::decayed_type OnCompleted>
    struct tap_t : public operators::details::lift_operator<tap_t<OnNext, OnError, OnCompleted>, OnNext, OnError, OnCompleted>
    {
        using operators::details::lift_operator<tap_t<OnNext, OnError, OnCompleted>, OnNext, OnError, OnCompleted>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            static_assert(rpp::constraint::invocable_r_v<void, OnNext, T>, "OnNext is not invocable with T");

            using result_type = T;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = tap_observer_strategy<TObserver, OnNext, OnError, OnCompleted>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = Prev;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Register callbacks to inspect observable emissions and perform side-effects
     *
     * @param on_error error handler
     *
     * @ingroup utility_operators
     * @see https://reactivex.io/documentation/operators/do.html
     */
    template<std::invocable<const std::exception_ptr&> OnError /* = rpp::utils::empty_function_t<std::exception_ptr> */>
        requires utils::is_not_template_callable<OnError>
    auto tap(OnError&& on_error)
    {
        using OnNext      = rpp::utils::empty_function_any_t;
        using OnCompleted = rpp::utils::empty_function_t<>;

        return details::tap_t<std::decay_t<OnNext>, std::decay_t<OnError>, std::decay_t<OnCompleted>>{
            OnNext{},
            std::forward<OnError>(on_error),
            OnCompleted{}};
    }

    /**
     * @brief Register callbacks to inspect observable emissions and perform side-effects
     *
     * @param on_completed completion handler
     *
     * @ingroup utility_operators
     * @see https://reactivex.io/documentation/operators/do.html
     */
    template<std::invocable<> OnCompleted /* = rpp::utils::empty_function_t<> */>
    auto tap(OnCompleted&& on_completed)
    {
        using OnNext  = rpp::utils::empty_function_any_t;
        using OnError = rpp::utils::empty_function_t<std::exception_ptr>;

        return details::tap_t<std::decay_t<OnNext>, std::decay_t<OnError>, std::decay_t<OnCompleted>>{
            OnNext{},
            OnError{},
            std::forward<OnCompleted>(on_completed)};
    }

    /**
     * @brief Register callbacks to inspect observable emissions and perform side-effects
     *
     * @param on_next next handler
     * @param on_completed completion handler
     *
     * @ingroup utility_operators
     * @see https://reactivex.io/documentation/operators/do.html
     */
    template<typename OnNext,
             std::invocable<> OnCompleted /* = rpp::utils::empty_function_t<> */>
    auto tap(OnNext&&      on_next,
             OnCompleted&& on_completed)
    {
        using OnError = rpp::utils::empty_function_t<std::exception_ptr>;

        return details::tap_t<std::decay_t<OnNext>, std::decay_t<OnError>, std::decay_t<OnCompleted>>{
            std::forward<OnNext>(on_next),
            OnError{},
            std::forward<OnCompleted>(on_completed)};
    }

    /**
     * @brief Register callbacks to inspect observable emissions and perform side-effects
     *
     * @param on_next next handler
     * @param on_error error handler
     * @param on_completed completion handler
     *
     * @ingroup utility_operators
     * @see https://reactivex.io/documentation/operators/do.html
     */
    template<typename OnNext /* = rpp::utils::empty_function_any_t */,
             std::invocable<const std::exception_ptr&> OnError /* = rpp::utils::empty_function_t<std::exception_ptr> */,
             std::invocable<>                          OnCompleted /* = rpp::utils::empty_function_t<> */>
        requires utils::is_not_template_callable<OnError>
    auto tap(OnNext&&      on_next /* = {} */,
             OnError&&     on_error /* = {} */,
             OnCompleted&& on_completed /* = {} */)
    {
        return details::tap_t<std::decay_t<OnNext>, std::decay_t<OnError>, std::decay_t<OnCompleted>>{
            std::forward<OnNext>(on_next),
            std::forward<OnError>(on_error),
            std::forward<OnCompleted>(on_completed)};
    }
} // namespace rpp::operators
