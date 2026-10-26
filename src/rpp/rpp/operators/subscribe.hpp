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
#include <rpp/observers/fwd.hpp>
#include <rpp/operators/fwd.hpp>

#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/disposables/disposable_wrapper.hpp>
#include <rpp/observers/observer.hpp>
#include <rpp/utils/functors.hpp>

#include <utility>

namespace rpp::operators::details
{
    template<typename... Args>
    class subscribe_t;

    template<rpp::constraint::decayed_type Type, rpp::constraint::observer_strategy<Type> ObserverStrategy>
    class subscribe_t<observer<Type, ObserverStrategy>>
    {
    public:
        explicit subscribe_t(observer<Type, ObserverStrategy>&& observer)
            : m_observer{std::move(observer)}
        {
        }

        template<rpp::constraint::observable_strategy<Type> Strategy>
        void operator()(const rpp::observable<Type, Strategy>& observable) &&
        {
            observable.subscribe(std::move(m_observer));
        }

    private:
        observer<Type, ObserverStrategy> m_observer;
    };

    template<rpp::constraint::observer_strategy_base ObserverStrategy>
    class subscribe_t<ObserverStrategy>
    {
    public:
        explicit subscribe_t(ObserverStrategy&& observer_strategy)
            : m_observer_strategy{std::move(observer_strategy)}
        {
        }

        explicit subscribe_t(const ObserverStrategy& observer_strategy)
            : m_observer_strategy{observer_strategy}
        {
        }

        template<rpp::constraint::observable Observable>
        void operator()(const Observable& observable) const &
        {
            static_assert(rpp::constraint::observer_strategy<ObserverStrategy, rpp::utils::extract_observable_type_t<Observable>>, "observer and observable should be of same type");
            observable.subscribe(m_observer_strategy);
        }

        template<rpp::constraint::observable Observable>
        void operator()(const Observable& observable) &&
        {
            static_assert(rpp::constraint::observer_strategy<ObserverStrategy, rpp::utils::extract_observable_type_t<Observable>>, "observer and observable should be of same type");
            observable.subscribe(std::move(m_observer_strategy));
        }

    private:
        ObserverStrategy m_observer_strategy;
    };

    template<rpp::constraint::decayed_type Type, rpp::constraint::observer_strategy<Type> ObserverStrategy>
    class subscribe_t<rpp::composite_disposable_wrapper, observer<Type, ObserverStrategy>>
    {
    public:
        explicit subscribe_t(rpp::composite_disposable_wrapper&& d, observer<Type, ObserverStrategy>&& observer)
            : m_disposable{std::move(d)}
            , m_observer{std::move(observer)}
        {
        }

        template<rpp::constraint::observable_strategy<Type> Strategy>
        rpp::composite_disposable_wrapper operator()(const rpp::observable<Type, Strategy>& observable) &&
        {
            observable.subscribe(observer_with_external_disposable<Type, observer<Type, ObserverStrategy>>{m_disposable, std::move(m_observer)});
            return m_disposable;
        }

    private:
        rpp::composite_disposable_wrapper m_disposable;
        observer<Type, ObserverStrategy>  m_observer;
    };

    template<rpp::constraint::observer_strategy_base ObserverStrategy>
    class subscribe_t<rpp::composite_disposable_wrapper, ObserverStrategy>
    {
    public:
        explicit subscribe_t(rpp::composite_disposable_wrapper&& d, ObserverStrategy&& observer_strategy)
            : m_disposable{std::move(d)}
            , m_observer_strategy{std::move(observer_strategy)}
        {
        }

        explicit subscribe_t(rpp::composite_disposable_wrapper&& d, const ObserverStrategy& observer_strategy)
            : m_disposable{std::move(d)}
            , m_observer_strategy{observer_strategy}
        {
        }

        template<rpp::constraint::observable Observable>
        rpp::composite_disposable_wrapper operator()(const Observable& observable) const &
        {
            static_assert(rpp::constraint::observer_strategy<ObserverStrategy, rpp::utils::extract_observable_type_t<Observable>>, "observer and observable should be of same type");
            observable.subscribe(m_disposable, m_observer_strategy);
            return m_disposable;
        }

        template<rpp::constraint::observable Observable>
        rpp::composite_disposable_wrapper operator()(const Observable& observable) &&
        {
            static_assert(rpp::constraint::observer_strategy<ObserverStrategy, rpp::utils::extract_observable_type_t<Observable>>, "observer and observable should be of same type");
            observable.subscribe(m_disposable, std::move(m_observer_strategy));
            return m_disposable;
        }

    private:
        rpp::composite_disposable_wrapper m_disposable;
        ObserverStrategy                  m_observer_strategy;
    };

    template<typename OnNext, std::invocable<const std::exception_ptr&> OnError, std::invocable<> OnCompleted>
    class subscribe_t<OnNext, OnError, OnCompleted>
    {
    public:
        template<rpp::constraint::decayed_same_as<OnNext> TOnNext, rpp::constraint::decayed_same_as<OnError> TOnError, rpp::constraint::decayed_same_as<OnCompleted> TOnCompleted>
            requires (!constraint::decayed_same_as<TOnNext, subscribe_t<OnNext, OnError, OnCompleted>>)
        explicit subscribe_t(TOnNext&& on_next, TOnError&& on_error, TOnCompleted&& on_completed)
            : m_on_next{std::forward<TOnNext>(on_next)}
            , m_on_error{std::forward<TOnError>(on_error)}
            , m_on_completed{std::forward<TOnCompleted>(on_completed)}
        {
        }

        template<rpp::constraint::decayed_type Type, rpp::constraint::observable_strategy<Type> Strategy>
        void operator()(const rpp::observable<Type, Strategy>& observable) &&
        {
            static_assert(std::invocable<OnNext, Type>, "OnNext should be suitable for type of observable");
            observable.subscribe(std::move(m_on_next), std::move(m_on_error), std::move(m_on_completed));
        }

        template<rpp::constraint::decayed_type Type, rpp::constraint::observable_strategy<Type> Strategy>
        void operator()(const rpp::observable<Type, Strategy>& observable) const &
        {
            static_assert(std::invocable<OnNext, Type>, "OnNext should be suitable for type of observable");
            observable.subscribe(m_on_next, m_on_error, m_on_completed);
        }

    private:
        RPP_NO_UNIQUE_ADDRESS OnNext      m_on_next;
        RPP_NO_UNIQUE_ADDRESS OnError     m_on_error;
        RPP_NO_UNIQUE_ADDRESS OnCompleted m_on_completed;
    };

    template<typename OnNext, std::invocable<const std::exception_ptr&> OnError, std::invocable<> OnCompleted>
    class subscribe_t<rpp::composite_disposable_wrapper, OnNext, OnError, OnCompleted>
    {
    public:
        template<rpp::constraint::decayed_same_as<OnNext> TOnNext, rpp::constraint::decayed_same_as<OnError> TOnError, rpp::constraint::decayed_same_as<OnCompleted> TOnCompleted>
        explicit subscribe_t(rpp::composite_disposable_wrapper d, TOnNext&& on_next, TOnError&& on_error, TOnCompleted&& on_completed)
            : m_disposable{std::move(d)}
            , m_on_next{std::forward<TOnNext>(on_next)}
            , m_on_error{std::forward<TOnError>(on_error)}
            , m_on_completed{std::forward<TOnCompleted>(on_completed)}
        {
        }

        template<rpp::constraint::decayed_type Type, rpp::constraint::observable_strategy<Type> Strategy>
        rpp::composite_disposable_wrapper operator()(const rpp::observable<Type, Strategy>& observable) &&
        {
            static_assert(std::invocable<OnNext, Type>, "OnNext should be suitable for type of observable");
            observable.subscribe(m_disposable, std::move(m_on_next), std::move(m_on_error), std::move(m_on_completed));
            return std::move(m_disposable);
        }

        template<rpp::constraint::decayed_type Type, rpp::constraint::observable_strategy<Type> Strategy>
        rpp::composite_disposable_wrapper operator()(const rpp::observable<Type, Strategy>& observable) const &
        {
            static_assert(std::invocable<OnNext, Type>, "OnNext should be suitable for type of observable");
            observable.subscribe(m_disposable, m_on_next, m_on_error, m_on_completed);
            return m_disposable;
        }

    private:
        rpp::composite_disposable_wrapper m_disposable;
        RPP_NO_UNIQUE_ADDRESS OnNext      m_on_next;
        RPP_NO_UNIQUE_ADDRESS OnError     m_on_error;
        RPP_NO_UNIQUE_ADDRESS OnCompleted m_on_completed;
    };

    template<typename... Args>
    subscribe_t(const Args&...) -> subscribe_t<Args...>;

    template<typename OnNext>
    concept on_next_like = (!rpp::utils::is_not_template_callable<OnNext> || std::invocable<OnNext, rpp::utils::convertible_to_any>) && (!rpp::constraint::decayed_same_as<OnNext, rpp::composite_disposable_wrapper> && !rpp::constraint::observer_strategy_base<OnNext> && !rpp::constraint::observer<OnNext>);
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Subscribes passed observer to emissions from this observable.
     *
     * @warning Observer must be moved in to subscribe method. (Not recommended) If you need to copy observer, convert
     * it to dynamic_observer
     *
     * @ingroup utility_operators
     */
    template<rpp::constraint::decayed_type Type, rpp::constraint::observer_strategy<Type> ObserverStrategy>
    auto subscribe(observer<Type, ObserverStrategy>&& observer)
    {
        return details::subscribe_t{std::move(observer)};
    }

    /**
     * @brief Subscribe passed observer to emissions from observable.
     * @details This overloading attaches passed disposable to observer and return it to provide ability to dispose observer early if needed.
     * @warning This overloading has some performance penalties, use it only when you really need to use disposable
     *
     * @param disposable is disposable to be attached to observer. If disposable is nullptr or disposed -> no any subscription happens
     *
     * @ingroup utility_operators
     */
    template<rpp::constraint::decayed_type Type, rpp::constraint::observer_strategy<Type> ObserverStrategy>
    auto subscribe(rpp::composite_disposable_wrapper disposable, observer<Type, ObserverStrategy>&& observer)
    {
        return details::subscribe_t{std::move(disposable), std::move(observer)};
    }

    /**
     * @brief Subscribes passed observer to emissions from this observable.
     *
     * @ingroup utility_operators
     */
    template<rpp::constraint::decayed_type Type>
    auto subscribe(dynamic_observer<Type> observer)
    {
        return details::subscribe_t{std::move(observer)};
    }

    /**
     * @brief  Subscribes passed observer strategy to emissions from this observable via construction of observer
     *
     * @ingroup utility_operators
     */
    template<rpp::constraint::observer_strategy_base ObserverStrategy>
        requires (!constraint::observer<ObserverStrategy>)
    auto subscribe(ObserverStrategy&& observer_strategy)
    {
        return details::subscribe_t{std::forward<ObserverStrategy>(observer_strategy)};
    }

    /**
     * @brief Subscribe passed observer to emissions from observable.
     * @details This overloading attaches passed disposable to observer and return it to provide ability to dispose observer early if needed.
     * @warning This overloading has some performance penalties, use it only when you really need to use disposable
     *
     * @param disposable is disposable to be attached to observer. If disposable is nullptr or disposed -> no any subscription happens
     *
     * @ingroup utility_operators
     */
    template<rpp::constraint::decayed_type Type>
    auto subscribe(rpp::composite_disposable_wrapper disposable, dynamic_observer<Type> observer)
    {
        return details::subscribe_t{std::move(disposable), std::move(observer)};
    }

    /**
     * @brief  Subscribes passed observer strategy to emissions from this observable via construction of observer
     * @details This overloading attaches passed disposable to observer and return it to provide ability to dispose observer early if needed.
     * @warning This overloading has some performance penalties, use it only when you really need to use disposable
     *
     * @param disposable is disposable to be attached to observer. If disposable is nullptr or disposed -> no any subscription happens
     *
     * @ingroup utility_operators
     */
    template<rpp::constraint::observer_strategy_base ObserverStrategy>
        requires (!constraint::observer<ObserverStrategy>)
    auto subscribe(rpp::composite_disposable_wrapper disposable, ObserverStrategy&& observer_strategy)
    {
        return details::subscribe_t{std::move(disposable), std::forward<ObserverStrategy>(observer_strategy)};
    }

    /**
     * @brief Construct rpp::lambda_observer on the fly and subscribe it to emissions from observable
     *
     * @ingroup utility_operators
     */
    template<details::on_next_like OnNext = rpp::utils::empty_function_any_t, std::invocable<const std::exception_ptr&> OnError = rpp::utils::rethrow_error_t, std::invocable<> OnCompleted = rpp::utils::empty_function_t<>>
    auto subscribe(OnNext&& on_next = {}, OnError&& on_error = {}, OnCompleted&& on_completed = {})
    {
        return details::subscribe_t{std::forward<OnNext>(on_next), std::forward<OnError>(on_error), std::forward<OnCompleted>(on_completed)};
    }

    /**
     * @brief Construct rpp::lambda_observer on the fly and subscribe it to emissions from observable
     *
     * @ingroup utility_operators
     */
    template<details::on_next_like OnNext, std::invocable<> OnCompleted>
    auto subscribe(OnNext&& on_next, OnCompleted&& on_completed)
    {
        return details::subscribe_t{std::forward<OnNext>(on_next), rpp::utils::rethrow_error_t{}, std::forward<OnCompleted>(on_completed)};
    }

    /**
     * @brief Construct rpp::lambda_observer on the fly and subscribe it to emissions from observable
     * @details This overloading attaches passed disposable to observer and return it to provide ability to dispose observer early if needed.
     * @warning This overloading has some performance penalties, use it only when you really need to use disposable
     *
     * @param d is disposable to be attached to observer. If disposable is nullptr or disposed -> no any subscription happens
     *
     * @ingroup utility_operators
     */
    template<details::on_next_like OnNext = rpp::utils::empty_function_any_t, std::invocable<const std::exception_ptr&> OnError = rpp::utils::rethrow_error_t, std::invocable<> OnCompleted = rpp::utils::empty_function_t<>>
    auto subscribe(rpp::composite_disposable_wrapper d, OnNext&& on_next = {}, OnError&& on_error = {}, OnCompleted&& on_completed = {})
    {
        return details::subscribe_t{std::move(d), std::forward<OnNext>(on_next), std::forward<OnError>(on_error), std::forward<OnCompleted>(on_completed)};
    }

    /**
     * @brief Construct rpp::lambda_observer on the fly and subscribe it to emissions from observable
     * @details This overloading attaches passed disposable to observer and return it to provide ability to dispose observer early if needed.
     * @warning This overloading has some performance penalties, use it only when you really need to use disposable
     *
     * @param d is disposable to be attached to observer. If disposable is nullptr or disposed -> no any subscription happens
     *
     * @ingroup utility_operators
     */
    template<details::on_next_like OnNext, std::invocable<> OnCompleted>
    auto subscribe(rpp::composite_disposable_wrapper d, OnNext&& on_next, OnCompleted&& on_completed)
    {
        return details::subscribe_t{std::move(d), std::forward<OnNext>(on_next), rpp::utils::rethrow_error_t{}, std::forward<OnCompleted>(on_completed)};
    }

    /**
     * @brief Subscribes passed observer to emissions from this observable.
     * @details This overloading attaches disposable to observer and return it to provide ability to dispose/disconnect observer early if needed.
     * @warning This overloading has some performance penalties, use it only when you really need to use disposable
     *
     * @warning Observer must be moved in to subscribe method. (Not recommended) If you need to copy observer, convert
     * it to dynamic_observer
     *
     * @ingroup utility_operators
     */
    template<rpp::constraint::decayed_type Type, rpp::constraint::observer_strategy<Type> ObserverStrategy>
    auto subscribe_with_disposable(observer<Type, ObserverStrategy>&& observer)
    {
        return subscribe(composite_disposable_wrapper::make(), std::move(observer));
    }

    /**
     * @brief Subscribes passed observer to emissions from this observable.
     * @details This overloading attaches disposable to observer and return it to provide ability to dispose/disconnect observer early if needed.
     * @warning This overloading has some performance penalties, use it only when you really need to use disposable
     *
     * @ingroup utility_operators
     */
    template<rpp::constraint::decayed_type Type>
    auto subscribe_with_disposable(dynamic_observer<Type> observer)
    {
        return subscribe(composite_disposable_wrapper::make(), std::move(observer));
    }

    /**
     * @brief Construct rpp::lambda_observer on the fly and subscribe it to emissions from observable
     * @details This overloading attaches disposable to observer and return it to provide ability to dispose/disconnect observer early if needed.
     * @warning This overloading has some performance penalties, use it only when you really need to use disposable
     *
     * @ingroup utility_operators
     */
    template<details::on_next_like OnNext = rpp::utils::empty_function_any_t, std::invocable<const std::exception_ptr&> OnError = rpp::utils::rethrow_error_t, std::invocable<> OnCompleted = rpp::utils::empty_function_t<>>
    auto subscribe_with_disposable(OnNext&& on_next = {}, OnError&& on_error = {}, OnCompleted&& on_completed = {})
    {
        return subscribe(composite_disposable_wrapper::make(), std::forward<OnNext>(on_next), std::forward<OnError>(on_error), std::forward<OnCompleted>(on_completed));
    }

    /**
     * @brief Construct rpp::lambda_observer on the fly and subscribe it to emissions from observable
     * @details This overloading attaches disposable to observer and return it to provide ability to dispose/disconnect observer early if needed.
     * @warning This overloading has some performance penalties, use it only when you really need to use disposable
     *
     * @ingroup utility_operators
     */
    template<details::on_next_like OnNext, std::invocable<> OnCompleted>
    auto subscribe_with_disposable(OnNext&& on_next, OnCompleted&& on_completed)
    {
        return subscribe(composite_disposable_wrapper::make(), std::forward<OnNext>(on_next), rpp::utils::rethrow_error_t{}, std::forward<OnCompleted>(on_completed));
    }
} // namespace rpp::operators
