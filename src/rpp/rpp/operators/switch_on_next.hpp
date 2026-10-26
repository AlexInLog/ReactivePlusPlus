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
#include <rpp/utils/utils.hpp>

#include <array>

namespace rpp::operators::details
{
    template<rpp::constraint::observer TObserver>
    class switch_on_next_state_t final : public rpp::details::base_disposable
    {
    public:
        template<rpp::constraint::decayed_same_as<TObserver> TObs>
            requires (!rpp::constraint::decayed_same_as<TObs, switch_on_next_state_t<TObserver>>)
        switch_on_next_state_t(TObs&& obs)
            : m_observer_with_mutex{std::forward<TObs>(obs)}
        {
        }

        switch_on_next_state_t(const switch_on_next_state_t&)     = delete;
        switch_on_next_state_t(switch_on_next_state_t&&) noexcept = delete;

        rpp::utils::pointer_under_lock<TObserver>                         get_observer() { return m_observer_with_mutex; }
        rpp::composite_disposable&                                        get_base_child_disposable() { return m_base_child_disposable; }
        rpp::utils::pointer_under_lock<rpp::composite_disposable_wrapper> get_inner_child_disposable() { return m_inner_child_disposable; }

    private:
        void base_dispose_impl(interface_disposable::Mode) noexcept override
        {
            get_base_child_disposable().dispose();
            get_inner_child_disposable()->dispose();
        }

    private:
        rpp::utils::value_with_mutex<TObserver>                         m_observer_with_mutex{};
        rpp::composite_disposable                                       m_base_child_disposable{};
        rpp::utils::value_with_mutex<rpp::composite_disposable_wrapper> m_inner_child_disposable{composite_disposable_wrapper::empty()};
    };

    template<rpp::constraint::observer TObserver>
    class switch_on_next_inner_observer_strategy
    {
    public:
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        switch_on_next_inner_observer_strategy(const std::shared_ptr<switch_on_next_state_t<TObserver>>& state, composite_disposable_wrapper&& refcounted)
            : m_state{state}
            , m_refcounted{std::move(refcounted)}
        {
        }

        template<typename T>
        void on_next(T&& v) const
        {
            m_state->get_observer()->on_next(std::forward<T>(v));
        }

        void on_error(const std::exception_ptr& err) const
        {
            m_state->get_observer()->on_error(err);
        }

        void on_completed() const
        {
            m_refcounted.dispose();
            if (m_state->get_base_child_disposable().is_disposed())
                m_state->get_observer()->on_completed();
        }

        void set_upstream(const disposable_wrapper& d) const { m_refcounted.add(d); }
        bool is_disposed() const { return m_refcounted.is_disposed(); }

    private:
        std::shared_ptr<switch_on_next_state_t<TObserver>> m_state;
        rpp::composite_disposable_wrapper                  m_refcounted;
    };

    template<rpp::constraint::observer TObserver>
    class switch_on_next_observer_strategy
    {
    public:
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        switch_on_next_observer_strategy(TObserver&& obs)
            : m_state{init_state(std::move(obs))}
        {
        }

        switch_on_next_observer_strategy(const switch_on_next_observer_strategy&)     = delete;
        switch_on_next_observer_strategy(switch_on_next_observer_strategy&&) noexcept = default;

        template<typename T>
        void on_next(T&& v) const
        {
            auto new_inner = rpp::composite_disposable_wrapper::make();
            {
                auto inner = m_state->get_inner_child_disposable();
                inner->dispose();
                if (m_state->is_disposed())
                    return;

                *inner = new_inner;
            }
            std::forward<T>(v).subscribe(switch_on_next_inner_observer_strategy<TObserver>{m_state, std::move(new_inner)});
        }

        void on_error(const std::exception_ptr& err) const
        {
            m_state->get_observer()->on_error(err);
        }

        void on_completed() const
        {
            m_state->get_base_child_disposable().dispose();
            if (m_state->get_inner_child_disposable()->is_disposed())
                m_state->get_observer()->on_completed();
        }

        void set_upstream(const disposable_wrapper& d) const { m_state->get_base_child_disposable().add(d); }
        bool is_disposed() const { return m_state->get_base_child_disposable().is_disposed(); }

    private:
        static std::shared_ptr<switch_on_next_state_t<TObserver>> init_state(TObserver&& observer)
        {
            const auto d   = disposable_wrapper_impl<switch_on_next_state_t<TObserver>>::make(std::move(observer));
            auto       ptr = d.lock();
            ptr->get_observer()->set_upstream(d.as_weak());
            return ptr;
        }

    private:
        std::shared_ptr<switch_on_next_state_t<TObserver>> m_state;
    };

    struct switch_on_next_t : lift_operator<switch_on_next_t>
    {
        using lift_operator<switch_on_next_t>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            static_assert(rpp::constraint::observable<T>, "T is not observable");

            using result_type = rpp::utils::extract_observable_type_t<T>;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = switch_on_next_observer_strategy<TObserver>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<1>;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Converts observable of observables into observable of values which emits values from most recent underlying observable till new observable obtained
     *
     * @marble switch_on_next
     {
         source observable:
         {
             +--1-2-3-5--|
             .....+4--6-9|
             .......+7-8-|
         }
         operator "switch_on_next" : +--1-24-7-8|
     }
     *
     * @details Actually this operator just unsubscribes from previous observable and subscribes on new observable when obtained in `on_next`
     *
     * @note `#include <rpp/operators/switch_on_next.hpp>`
     *
     * @par Example:
     * @snippet switch_on_next.cpp switch_on_next
     *
     * @ingroup combining_operators
     * @see https://reactivex.io/documentation/operators/switch.html
     */
    inline auto switch_on_next()
    {
        return details::switch_on_next_t{};
    }
} // namespace rpp::operators
