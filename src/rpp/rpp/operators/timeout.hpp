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

#include <rpp/operators/details/strategy.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/utils/exceptions.hpp>
#include <rpp/utils/utils.hpp>

namespace rpp::operators::details
{
    template<rpp::constraint::observer TObserver, rpp::constraint::observable TFallbackObservable, rpp::details::disposables::constraint::disposables_container Container>
    class timeout_disposable final : public rpp::composite_disposable_impl<Container>
    {
    public:
        struct observer_with_timeout
        {
            RPP_NO_UNIQUE_ADDRESS TObserver observer;
            rpp::schedulers::time_point     timeout;
        };

        timeout_disposable(TObserver&& observer, rpp::schedulers::duration period, const TFallbackObservable& fallback, rpp::schedulers::time_point timeout)
            : m_observer_with_timeout{observer_with_timeout{std::move(observer), timeout}}
            , m_period{period}
            , m_fallback{fallback}
        {
        }
        rpp::utils::pointer_under_lock<observer_with_timeout> get_observer_with_timeout_under_lock() { return m_observer_with_timeout; }

        const TFallbackObservable& get_fallback() const { return m_fallback; }

        rpp::schedulers::duration get_period() const { return m_period; }

    private:
        rpp::utils::value_with_mutex<observer_with_timeout> m_observer_with_timeout;

        const rpp::schedulers::duration                 m_period;
        RPP_NO_UNIQUE_ADDRESS const TFallbackObservable m_fallback;
    };

    template<rpp::constraint::observer TObserver, rpp::constraint::observable TFallbackObservable, rpp::details::disposables::constraint::disposables_container Container>
    struct timeout_disposable_wrapper
    {
        std::shared_ptr<timeout_disposable<TObserver, TFallbackObservable, Container>> disposable;

        bool is_disposed() const { return disposable->is_disposed(); }

        void on_error(const std::exception_ptr& err) const
        {
            disposable->get_observer_with_timeout_under_lock()->observer.on_error(err);
        }
    };

    template<rpp::constraint::observer TObserver, rpp::constraint::observable TFallbackObservable, rpp::details::disposables::constraint::disposables_container Container, rpp::schedulers::constraint::scheduler TScheduler>
    struct timeout_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        std::shared_ptr<timeout_disposable<TObserver, TFallbackObservable, Container>> disposable;

        void set_upstream(const rpp::disposable_wrapper& d) const
        {
            disposable->add(d);
        }

        bool is_disposed() const
        {
            return disposable->is_disposed();
        }

        template<typename T>
        void on_next(T&& v) const
        {
            auto obs_with_timeout = disposable->get_observer_with_timeout_under_lock();
            obs_with_timeout->observer.on_next(std::forward<T>(v));
            obs_with_timeout->timeout = rpp::schedulers::utils::get_worker_t<TScheduler>::now() + disposable->get_period();
        }

        void on_error(const std::exception_ptr& err) const noexcept
        {
            const auto obs_with_timeout = disposable->get_observer_with_timeout_under_lock();
            if (!disposable->is_disposed())
                obs_with_timeout->observer.on_error(err);
        }

        void on_completed() const noexcept
        {
            const auto obs_with_timeout = disposable->get_observer_with_timeout_under_lock();
            if (!disposable->is_disposed())
                obs_with_timeout->observer.on_completed();
        }
    };

    template<rpp::constraint::observable TFallbackObservable, rpp::schedulers::constraint::scheduler TScheduler>
    struct timeout_t
    {
        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            static_assert(rpp::constraint::observable_of_type<TFallbackObservable, T>, "TFallbackObservable should be the same type as T");

            using result_type = T;

            constexpr static bool own_current_queue = true;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<1>;

        rpp::schedulers::duration                 period;
        RPP_NO_UNIQUE_ADDRESS TFallbackObservable fallback;
        RPP_NO_UNIQUE_ADDRESS TScheduler          scheduler;

        template<rpp::constraint::decayed_type Type, rpp::details::observables::constraint::disposables_strategy DisposableStrategy, rpp::constraint::observer Observer>
        auto lift_with_disposables_strategy(Observer&& observer) const
        {
            using worker_t  = rpp::schedulers::utils::get_worker_t<TScheduler>;
            using container = typename DisposableStrategy::disposables_container;

            const auto timeout = worker_t::now() + period;

            const auto disposable = disposable_wrapper_impl<timeout_disposable<std::decay_t<Observer>, TFallbackObservable, container>>::make(std::forward<Observer>(observer), period, fallback, timeout);
            auto       ptr        = disposable.lock();
            ptr->get_observer_with_timeout_under_lock()->observer.set_upstream(disposable.as_weak());

            const auto worker = scheduler.create_worker();
            using wrapper     = timeout_disposable_wrapper<std::decay_t<Observer>, TFallbackObservable, container>;
            worker.schedule(
                timeout,
                [](wrapper& handler) -> rpp::schedulers::optional_delay_to {
                    auto locked_obs_with_timeout = handler.disposable->get_observer_with_timeout_under_lock();
                    if (rpp::schedulers::utils::get_worker_t<TScheduler>::now() < locked_obs_with_timeout->timeout)
                        return rpp::schedulers::delay_to(locked_obs_with_timeout->timeout);

                    if (!handler.disposable->is_disposed())
                    {
                        handler.disposable->dispose();
                        handler.disposable->get_fallback().subscribe(std::move(locked_obs_with_timeout->observer));
                    }
                    return std::nullopt;
                },
                wrapper{ptr});

            return rpp::observer<Type, timeout_observer_strategy<std::decay_t<Observer>, TFallbackObservable, container, TScheduler>>{std::move(ptr)};
        }
    };

    template<rpp::schedulers::constraint::scheduler TScheduler>
    struct timeout_with_error_t
    {
        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            using result_type = T;

            constexpr static bool own_current_queue = true;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = typename timeout_t<rpp::error_observable<int>, TScheduler>::template updated_optimal_disposables_strategy<Prev>;

        rpp::schedulers::duration        period;
        RPP_NO_UNIQUE_ADDRESS TScheduler scheduler;

        template<rpp::constraint::decayed_type Type, rpp::details::observables::constraint::disposables_strategy DisposableStrategy, rpp::constraint::observer Observer>
        auto lift_with_disposables_strategy(Observer&& observer) const
        {
            return timeout_t<rpp::error_observable<Type>, TScheduler>{period, rpp::source::error<rpp::utils::extract_observer_type_t<Observer>>(std::make_exception_ptr(rpp::utils::timeout_reached{"Timeout reached"})), scheduler}
                .template lift_with_disposables_strategy<Type, DisposableStrategy>(std::forward<Observer>(observer));
        }
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Forwards emissions from original observable, but subscribes on fallback observable if no any events during specified period of time (since last emission)
     *
     * @marble timeout_fallback_obs
     {
         source observable            : +--1-2-3-4--------5-|
         operator "timeout(4, -10-|)" : +--1-2-3-4----10-|
     }
     *
     * @param period is maximum duration between emitted items before a timeout occurs
     * @param fallback_observable is observable to subscribe on when timeout reached
     * @param scheduler is scheduler used to run timer for timeout
     * @note `#include <rpp/operators/timeout.hpp>`
     *
     * @par Example
     * @snippet timeout.cpp fallback_observable
     *
     * @ingroup utility_operators
     * @see https://reactivex.io/documentation/operators/timeout.html
     */
    template<rpp::constraint::observable TFallbackObservable, rpp::schedulers::constraint::scheduler TScheduler>
    auto timeout(rpp::schedulers::duration period, TFallbackObservable&& fallback_observable, const TScheduler& scheduler)
    {
        return details::timeout_t<std::decay_t<TFallbackObservable>, TScheduler>{period, std::forward<TFallbackObservable>(fallback_observable), scheduler};
    }

    /**
     * @brief Forwards emissions from original observable, but emit error if no any events during specified period of time (since last emission)
     *
     * @marble timeout_default
        {
            source observable     : +--1-2-3-4------5-|
            operator "timeout(4)" : +--1-2-3-4----#
        }
     * @param period is maximum duration between emitted items before a timeout occurs
     * @param scheduler is scheduler used to run timer for timeout
     * @note `#include <rpp/operators/timeout.hpp>`
     *
     * @par Example
     * @snippet timeout.cpp default
 *
     * @ingroup utility_operators
     * @see https://reactivex.io/documentation/operators/timeout.html
     */
    template<rpp::schedulers::constraint::scheduler TScheduler>
    auto timeout(rpp::schedulers::duration period, const TScheduler& scheduler)
    {
        return details::timeout_with_error_t<TScheduler>{period, scheduler};
    }
} // namespace rpp::operators
