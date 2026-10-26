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
#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/schedulers/current_thread.hpp>
#include <rpp/utils/utils.hpp>

namespace rpp::operators::details
{
    template<rpp::constraint::observer TObserver>
    class take_until_disposable final : public rpp::composite_disposable
    {
    public:
        take_until_disposable(TObserver&& observer)
            : m_observer_with_mutex(std::move(observer))
        {
        }

        take_until_disposable(const TObserver& observer)
            : m_observer_with_mutex(observer)
        {
        }

        bool is_stopped() const { return m_stopped; }
        bool stop_return_was_stopped() { return m_stopped.exchange(true); }

        rpp::utils::pointer_under_lock<TObserver> get_observer() { return m_observer_with_mutex; }

    private:
        rpp::utils::value_with_mutex<TObserver> m_observer_with_mutex{};
        std::atomic_bool                        m_stopped{};
    };

    template<rpp::constraint::observer TObserver>
    struct take_until_observer_strategy_base
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::Auto;

        std::shared_ptr<take_until_disposable<TObserver>> state;

        void on_error(const std::exception_ptr& err) const
        {
            if (!state->stop_return_was_stopped())
                state->get_observer()->on_error(err);
        }

        void on_completed() const
        {
            if (!state->stop_return_was_stopped())
                state->get_observer()->on_completed();
        }

        void set_upstream(const disposable_wrapper& d) { state->add(d); }

        bool is_disposed() const { return state->is_disposed(); }
    };

    template<rpp::constraint::observer TObserver>
    struct take_until_throttle_observer_strategy : public take_until_observer_strategy_base<TObserver>
    {
        template<typename T>
        void on_next(const T&) const
        {
            if (!take_until_observer_strategy_base<TObserver>::state->stop_return_was_stopped())
                take_until_observer_strategy_base<TObserver>::state->get_observer()->on_completed();
        }
    };

    template<rpp::constraint::observer TObserver>
    struct take_until_observer_strategy : public take_until_observer_strategy_base<TObserver>
    {
        template<typename T>
        void on_next(T&& v) const
        {
            if (!take_until_observer_strategy_base<TObserver>::state->is_stopped())
                take_until_observer_strategy_base<TObserver>::state->get_observer()->on_next(std::forward<T>(v));
        }
    };

    template<rpp::constraint::observable TObservable>
    struct take_until_t
    {
        RPP_NO_UNIQUE_ADDRESS TObservable observable{};

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            using result_type = T;

            constexpr static bool own_current_queue = true;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<1>;

        template<rpp::constraint::decayed_type Type, rpp::constraint::observer Observer>
        auto lift(Observer&& observer) const
        {
            const auto d   = disposable_wrapper_impl<take_until_disposable<std::decay_t<Observer>>>::make(std::forward<Observer>(observer));
            auto       ptr = d.lock();
            ptr->get_observer()->set_upstream(d.as_weak());

            observable.subscribe(take_until_throttle_observer_strategy<std::decay_t<Observer>>{ptr});
            return rpp::observer<Type, take_until_observer_strategy<std::decay_t<Observer>>>(std::move(ptr));
        }
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Discard any items emitted by an Observable after a second Observable emits an item or terminates.
     * @warning The take_until subscribes and begins mirroring the source Observable. It also monitors a second Observable that you provide. If this second Observable emits an item or sends a on_error/on_completed notification, the Observable returned by take_until stops mirroring the source Observable and terminates.
     *
     * @marble take_until
     {
         source observable        : +-1--2--3--|
         source until_observable  : +---s--s---|
         operator "take_until"    : +-1-|
     }
     *
     * @details Actually this operator just subscribes on 2 observables and completes original when `until_observable` emits any value
     *
     * @param until_observable is the observables that stops the source observable from sending values when it emits one value or sends a on_error/on_completed event.
     * @note `#include <rpp/operators/take_until.hpp>`
     *
     * @par Examples
     * @snippet take_until.cpp take_until
     * @snippet take_until.cpp terminate
     *
     * @ingroup conditional_operators
     * @see https://reactivex.io/documentation/operators/takeuntil.html
     */
    template<rpp::constraint::observable TObservable>
    auto take_until(TObservable&& until_observable)
    {
        return details::take_until_t<std::decay_t<TObservable>>{std::forward<TObservable>(until_observable)};
    }
} // namespace rpp::operators
