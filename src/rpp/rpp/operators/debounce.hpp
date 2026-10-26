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

#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/operators/details/strategy.hpp>
#include <rpp/utils/utils.hpp>

namespace rpp::operators::details
{
    template<rpp::constraint::observer Observer, typename Worker, rpp::details::disposables::constraint::disposables_container Container>
    class debounce_disposable;

    template<rpp::constraint::observer Observer, typename Worker, rpp::details::disposables::constraint::disposables_container Container>
    struct debounce_disposable_wrapper
    {
        std::shared_ptr<debounce_disposable<Observer, Worker, Container>> disposable{};

        bool is_disposed() const { return disposable->is_disposed(); }

        void on_error(const std::exception_ptr& err) const { disposable->get_observer_under_lock()->on_error(err); }
    };

    template<rpp::constraint::observer Observer, typename Worker, rpp::details::disposables::constraint::disposables_container Container>
    class debounce_disposable final : public rpp::composite_disposable_impl<Container>
        , public rpp::details::enable_wrapper_from_this<debounce_disposable<Observer, Worker, Container>>
    {
        using T = rpp::utils::extract_observer_type_t<Observer>;

    public:
        debounce_disposable(Observer&& in_observer, Worker&& in_worker, rpp::schedulers::duration period)
            : m_observer(std::move(in_observer))
            , m_worker{std::move(in_worker)}
            , m_period{period}
        {
        }

        template<typename TT>
        void emplace_safe(TT&& v)
        {
            std::lock_guard lock{m_mutex};
            m_value_to_be_emitted.emplace(std::forward<TT>(v));
            const bool need_to_scheduled        = !m_time_when_value_should_be_emitted.has_value();
            m_time_when_value_should_be_emitted = m_worker.now() + m_period;
            if (need_to_scheduled)
            {
                schedule();
            }
        }

        std::optional<T> extract_value()
        {
            std::lock_guard lock{m_mutex};
            return std::exchange(m_value_to_be_emitted, std::optional<T>{});
        }

        rpp::utils::pointer_under_lock<Observer> get_observer_under_lock() { return m_observer; }

    private:
        void schedule()
        {
            m_worker.schedule(
                m_time_when_value_should_be_emitted.value(),
                [](const debounce_disposable_wrapper<Observer, Worker, Container>& handler) -> schedulers::optional_delay_to {
                    auto value_or_duration = handler.disposable->extract_value_or_time();
                    if (auto* timepoint = std::get_if<schedulers::time_point>(&value_or_duration))
                        return schedulers::optional_delay_to{*timepoint};

                    if (auto* value = std::get_if<T>(&value_or_duration))
                        handler.disposable->get_observer_under_lock()->on_next(std::move(*value));

                    return std::nullopt;
                },
                debounce_disposable_wrapper<Observer, Worker, Container>{this->wrapper_from_this().lock()});
        }

        std::variant<std::monostate, T, schedulers::time_point> extract_value_or_time()
        {
            std::lock_guard lock{m_mutex};
            if (!m_time_when_value_should_be_emitted.has_value() || !m_value_to_be_emitted.has_value())
                return std::monostate{};

            if (m_time_when_value_should_be_emitted > m_worker.now())
                return m_time_when_value_should_be_emitted.value();

            m_time_when_value_should_be_emitted.reset();
            auto v = std::move(m_value_to_be_emitted).value();
            m_value_to_be_emitted.reset();
            return v;
        }

        rpp::utils::value_with_mutex<Observer> m_observer;
        RPP_NO_UNIQUE_ADDRESS Worker           m_worker;
        rpp::schedulers::duration              m_period;

        std::mutex                            m_mutex{};
        std::optional<schedulers::time_point> m_time_when_value_should_be_emitted{};
        std::optional<T>                      m_value_to_be_emitted{};
    };

    template<rpp::constraint::observer Observer, typename Worker, rpp::details::disposables::constraint::disposables_container Container>
    struct debounce_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        std::shared_ptr<debounce_disposable<Observer, Worker, Container>> disposable{};

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
            disposable->emplace_safe(std::forward<T>(v));
        }

        void on_error(const std::exception_ptr& err) const noexcept
        {
            disposable->get_observer_under_lock()->on_error(err);
        }

        void on_completed() const noexcept
        {
            const auto observer = disposable->get_observer_under_lock();
            if (const auto value = disposable->extract_value())
                observer->on_next(std::move(value).value());
            observer->on_completed();
        }
    };

    template<rpp::schedulers::constraint::scheduler Scheduler>
    struct debounce_t
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


            const auto disposable = disposable_wrapper_impl<debounce_disposable<std::decay_t<Observer>, worker_t, container>>::make(std::forward<Observer>(observer), scheduler.create_worker(), duration);
            auto       ptr        = disposable.lock();
            ptr->get_observer_under_lock()->set_upstream(disposable.as_weak());
            return rpp::observer<Type, debounce_observer_strategy<std::decay_t<Observer>, worker_t, container>>{std::move(ptr)};
        }
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Only emit emission if specified period of time has passed without any other emission. On each new emission timer reset.
     *
     * @marble debounce
     {
         source    observable   : +--1-2-----3---|
         operator "debounce(4)" : +--------2-----3|
     }
     *
     * @details Actually this operator resets time of last emission, schedules action to send this emission after specified period if no any new emissions till this moment.
     *
     * @param period is duration of time should be passed since emission from original observable without any new emissions to emit this emission.
     * @param scheduler is scheduler used to run timer for debounce

     * @note `#include <rpp/operators/debounce.hpp>`
     *
     * @par Example
     * @snippet debounce.cpp debounce
     *
     * @ingroup utility_operators
     * @see https://reactivex.io/documentation/operators/debounce.html
     */
    template<rpp::schedulers::constraint::scheduler Scheduler>
    auto debounce(rpp::schedulers::duration period, Scheduler&& scheduler)
    {
        return details::debounce_t<std::decay_t<Scheduler>>{period, std::forward<Scheduler>(scheduler)};
    }
} // namespace rpp::operators
