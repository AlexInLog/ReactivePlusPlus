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

#include <rpp/defs.hpp>
#include <rpp/operators/details/strategy.hpp>
#include <rpp/utils/constraints.hpp>
#include <rpp/utils/utils.hpp>

#include "rpp/observers/fwd.hpp"

namespace rpp::operators::details
{
    template<rpp::constraint::observer TObserver,
             typename TObservable,
             typename TNotifier>
    struct repeating_state final : public rpp::composite_disposable
    {
        repeating_state(TObserver&& observer, const TObservable& observable, const TNotifier& notifier)
            : observer(std::move(observer))
            , observable(observable)
            , notifier(notifier)
        {
        }

        std::atomic_bool is_inside_drain{};

        RPP_NO_UNIQUE_ADDRESS TObserver   observer;
        RPP_NO_UNIQUE_ADDRESS TObservable observable;
        RPP_NO_UNIQUE_ADDRESS TNotifier   notifier;
    };

    template<typename TStrategy, rpp::constraint::observer TObserver, typename TObservable, typename TNotifier>
    void drain(const std::shared_ptr<repeating_state<TObserver, TObservable, TNotifier>>& state);

    template<typename TOuterStrategy,
             rpp::constraint::observer TObserver,
             typename TObservable,
             typename TNotifier>
    struct repeating_inner_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        std::shared_ptr<repeating_state<TObserver, TObservable, TNotifier>> state;
        mutable bool                                                        locally_disposed{};

        template<typename T>
        void on_next(T&&) const
        {
            locally_disposed = true;
            state->clear();

            if (state->is_inside_drain.exchange(false, std::memory_order::seq_cst))
                return;

            drain<TOuterStrategy>(state);
        }

        void on_error(const std::exception_ptr& err) const
        {
            locally_disposed = true;
            state->observer.on_error(err);
        }

        void on_completed() const
        {
            locally_disposed = true;
            state->observer.on_completed();
        }

        void set_upstream(const disposable_wrapper& d) const { state->add(d); }

        bool is_disposed() const { return locally_disposed || state->is_disposed(); }
    };

    template<rpp::constraint::observer TObserver,
             typename TObservable,
             typename TNotifier>
    struct repeating_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        std::shared_ptr<repeating_state<TObserver, TObservable, TNotifier>> state;

        void set_upstream(const disposable_wrapper& d) { state->add(d); }

        bool is_disposed() const { return state->is_disposed(); }
    };

    template<typename TStrategy, rpp::constraint::observer TObserver, typename TObservable, typename TNotifier>
    void drain(const std::shared_ptr<repeating_state<TObserver, TObservable, TNotifier>>& state)
    {
        while (!state->is_disposed())
        {
            state->is_inside_drain.store(true, std::memory_order::seq_cst);
            try
            {
                using value_type = rpp::utils::extract_observer_type_t<TObserver>;
                state->observable.subscribe(rpp::observer<value_type, TStrategy>{state});

                if (state->is_inside_drain.exchange(false, std::memory_order::seq_cst))
                    return;
            }
            catch (...)
            {
                state->observer.on_error(std::current_exception());
                return;
            }
        }
    }
} // namespace rpp::operators::details
