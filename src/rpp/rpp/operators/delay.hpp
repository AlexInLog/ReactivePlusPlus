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
#include <rpp/operators/details/strategy.hpp>

#include <mutex>
#include <queue>

namespace rpp::operators::details
{
    template<typename T>
    struct emission
    {
        template<typename TT>
        emission(TT&& item, schedulers::time_point time)
            : value{std::forward<TT>(item)}
            , time_point{time}
        {
        }

        std::variant<T, std::exception_ptr, rpp::utils::none> value{};
        rpp::schedulers::time_point                           time_point{};
    };

    template<rpp::constraint::observer Observer, typename Worker, rpp::details::disposables::constraint::disposables_container Container>
    struct delay_disposable final : public rpp::composite_disposable_impl<Container>
    {
        using T = rpp::utils::extract_observer_type_t<Observer>;

        delay_disposable(Observer&& in_observer, Worker&& in_worker, rpp::schedulers::duration delay)
            : observer(std::move(in_observer))
            , worker{std::move(in_worker)}
            , delay{delay}
        {
        }

        RPP_NO_UNIQUE_ADDRESS Observer observer;
        RPP_NO_UNIQUE_ADDRESS Worker   worker;
        rpp::schedulers::duration      delay;

        std::mutex              mutex{};
        std::queue<emission<T>> queue;
        bool                    is_active{};
    };

    template<rpp::constraint::observer Observer, typename Worker, rpp::details::disposables::constraint::disposables_container Container>
    struct delay_disposable_wrapper
    {
        std::shared_ptr<delay_disposable<Observer, Worker, Container>> disposable{};

        bool is_disposed() const { return disposable->is_disposed(); }

        void on_error(const std::exception_ptr& err) const { disposable->observer.on_error(err); }
    };

    template<rpp::constraint::observer Observer, typename Worker, rpp::details::disposables::constraint::disposables_container Container, bool ClearOnError>
    struct delay_observer_strategy
    {
        static constexpr auto                                          preferred_disposables_mode = rpp::details::observers::disposables_mode::Boolean;
        std::shared_ptr<delay_disposable<Observer, Worker, Container>> disposable{};

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
            emplace(std::forward<T>(v));
        }

        void on_error(const std::exception_ptr& err) const noexcept
        {
            emplace(err);
            disposable->clear();
        }

        void on_completed() const noexcept
        {
            emplace(rpp::utils::none{});
            disposable->clear();
        }

    private:
        template<typename TT>
        void emplace(TT&& value) const
        {
            if (const auto tp = emplace_safe(std::forward<TT>(value)))
            {
                disposable->worker.schedule(
                    tp.value(),
                    [](const delay_disposable_wrapper<Observer, Worker, Container>& wrapper) { return drain_queue(wrapper.disposable); },
                    delay_disposable_wrapper<Observer, Worker, Container>{disposable});
            }
        }

        template<typename TT>
        std::optional<rpp::schedulers::time_point> emplace_safe(TT&& item) const
        {
            std::lock_guard lock{disposable->mutex};
            if constexpr (ClearOnError && rpp::constraint::decayed_same_as<std::exception_ptr, TT>)
            {
                disposable->queue = std::queue<emission<rpp::utils::extract_observer_type_t<Observer>>>{};
                disposable->observer.on_error(std::forward<TT>(item));
                return std::nullopt;
            }
            else
            {
                const auto tp = disposable->worker.now() + disposable->delay;
                disposable->queue.emplace(std::forward<TT>(item), tp);
                if (!disposable->is_active)
                {
                    disposable->is_active = true;
                    return tp;
                }
                return std::nullopt;
            }
        }

        static schedulers::optional_delay_to drain_queue(const std::shared_ptr<delay_disposable<Observer, Worker, Container>>& disposable)
        {
            for (bool just_schedule = false;; just_schedule = true)
            {
                std::unique_lock lock{disposable->mutex};
                if (disposable->queue.empty())
                {
                    disposable->is_active = false;
                    return std::nullopt;
                }

                auto& top = disposable->queue.front();
                if (just_schedule || top.time_point > disposable->worker.now())
                    return schedulers::optional_delay_to{top.time_point};

                auto item = std::move(top.value);
                disposable->queue.pop();
                lock.unlock();

                std::visit(rpp::utils::overloaded{[&](rpp::utils::extract_observer_type_t<Observer>&& v) { disposable->observer.on_next(std::move(v)); },
                                                  [&](const std::exception_ptr& err) { disposable->observer.on_error(err); },
                                                  [&](rpp::utils::none) {
                                                      disposable->observer.on_completed();
                                                  }},
                           std::move(item));
            }
        }
    };

    template<rpp::schedulers::constraint::scheduler Scheduler, bool ClearOnError>
    struct delay_t
    {
        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            using result_type = T;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<1>;

        rpp::schedulers::duration       duration;
        RPP_NO_UNIQUE_ADDRESS Scheduler scheduler;

        template<rpp::constraint::decayed_type Type, rpp::details::observables::constraint::disposables_strategy DisposableStrategy, rpp::constraint::observer Observer>
        auto lift_with_disposables_strategy(Observer&& observer) const
        {
            using worker_t  = rpp::schedulers::utils::get_worker_t<Scheduler>;
            using container = typename DisposableStrategy::disposables_container;

            const auto disposable = disposable_wrapper_impl<delay_disposable<std::decay_t<Observer>, worker_t, container>>::make(std::forward<Observer>(observer), scheduler.create_worker(), duration);
            auto       ptr        = disposable.lock();
            ptr->observer.set_upstream(disposable.as_weak());
            return rpp::observer<Type, delay_observer_strategy<std::decay_t<Observer>, worker_t, container, ClearOnError>>{std::move(ptr)};
        }
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Shift the emissions from an Observable forward in time by a particular amount.
     * @details The delay operator modifies its source Observable by pausing for a particular increment of time (that you specify) before emitting each of the source Observable’s items. This has the effect of shifting the entire sequence of items emitted by the Observable forward in time by that specified increment.
     *
     * @marble delay
        {
            source observable      : +-1-2-3-#
            operator "delay:(--)"  : +---1-2-3-#
        }
     *
     * @details Actually this operator just schedules emissions via provided scheduler with provided delay_duration.
     * @warning on_error/on_completed invoking also would be delayed as any other emissions, so, WHOLE observable would be shifter. If you want to obtain `on_error` immediately, use `observe_on` instead.
     *
     * @param delay_duration is the delay duration for emitting items. Delay duration should be able to cast to rpp::schedulers::duration.
     * @param scheduler provides the threading model for delay. e.g. With a new thread scheduler, the observer sees the values in a new thread after a delay duration to the subscription.
     * @note `#include <rpp/operators/delay.hpp>`
     *
     * @par Examples
     * @snippet delay.cpp delay
     *
     * @ingroup utility_operators
     * @see https://reactivex.io/documentation/operators/delay.html
     */
    template<rpp::schedulers::constraint::scheduler Scheduler>
    auto delay(rpp::schedulers::duration delay_duration, Scheduler&& scheduler)
    {
        return details::delay_t<std::decay_t<Scheduler>, false>{delay_duration, std::forward<Scheduler>(scheduler)};
    }
} // namespace rpp::operators
