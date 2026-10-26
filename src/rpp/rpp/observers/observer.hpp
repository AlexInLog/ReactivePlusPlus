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

#include <rpp/observers/fwd.hpp>

#include <rpp/defs.hpp>
#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/disposables/disposable_wrapper.hpp>
#include <rpp/observers/details/disposables_strategy.hpp>
#include <rpp/utils/exceptions.hpp>
#include <rpp/utils/functors.hpp>
#include <rpp/utils/utils.hpp>

#include <exception>

namespace rpp::details
{
    template<constraint::decayed_type Type, constraint::observer_strategy<Type> Strategy, observers::constraint::disposables_strategy DisposablesStrategy>
    class observer_impl
    {
    protected:
        template<typename... Args>
            requires constraint::is_constructible_from<Strategy, Args&&...>
        explicit observer_impl(DisposablesStrategy strategy, Args&&... args)
            : m_strategy{std::forward<Args>(args)...}
            , m_disposable{std::move(strategy)}
        {
        }

    public:
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        using on_next_lvalue = void (observer_impl::*)(const Type&) const noexcept;
        using on_next_rvalue = void (observer_impl::*)(Type&&) const noexcept;

        /**
         * @brief Observable calls this method to pass disposable. Observer disposes this disposable WHEN observer wants to unsubscribe.
         * @note This method can be called multiple times.
         */
        void set_upstream(const disposable_wrapper& d) noexcept
        {
            if (is_disposed())
            {
                d.dispose();
                return;
            }

            try
            {
                m_disposable.add(d);
                m_strategy.set_upstream(d);
            }
            catch (...)
            {
                on_error(std::current_exception());
            }
        }

        /**
         * @brief Observable calls this method to check if observer interested or not in emissions
         *
         * @return true if observer disposed and no longer interested in emissions
         * @return false if observer still interested in emissions
         */
        bool is_disposed() const noexcept
        {
            return m_disposable.is_disposed() || m_strategy.is_disposed();
        }

        /**
         * @brief Observable calls this method to notify observer about new value.
         *
         * @note obtains value by const-reference to original object.
         */
        void on_next(const Type& v) const noexcept
        {
            try
            {
                if (!is_disposed())
                    m_strategy.on_next(v);
            }
            catch (...)
            {
                on_error(std::current_exception());
            }
        }

        /**
         * @brief Observable calls this method to notify observer about new value.
         *
         * @note obtains value by rvalue-reference to original object
         */
        void on_next(Type&& v) const noexcept
        {
            try
            {
                if (!is_disposed())
                    m_strategy.on_next(std::move(v));
            }
            catch (...)
            {
                on_error(std::current_exception());
            }
        }

        /**
         * @brief Observable calls this method to notify observer about some error during generation next data.
         * @invariant Obtaining of this call means no any further on_next/on_error or on_completed calls from this Observable
         * @param err details of error
         */
        void on_error(const std::exception_ptr& err) const noexcept
        {
            if (!is_disposed())
            {
                rpp::utils::finally_action finally{[&] {
                    m_disposable.dispose();
                }};
                m_strategy.on_error(err);
            }
        }

        /**
         * @brief Observable calls this method to notify observer about completion of emissions.
         * @invariant Obtaining of this call means no any further on_next/on_error or on_completed calls from this Observable
         */
        void on_completed() const noexcept
        {
            if (!is_disposed())
            {
                rpp::utils::finally_action finally{[&] {
                    m_disposable.dispose();
                }};
                m_strategy.on_completed();
            }
        }

    private:
        RPP_NO_UNIQUE_ADDRESS Strategy                    m_strategy;
        RPP_NO_UNIQUE_ADDRESS mutable DisposablesStrategy m_disposable;
    };
} // namespace rpp::details

namespace rpp
{
    /**
     * @brief Base class for any observer used in RPP. It handles core callbacks of observers. Objects of this class would be passed to subscribe of observable
     *
     * @warning By default observer is not copyable, only movable. If you need to COPY your observer, you need to convert it to rpp::dynamic_observer via rpp::observer::as_dynamic
     * @warning Expected that observer would be subscribed only to ONE observable ever. It can keep internal state and track it it was disposed or not. So, subscribing same observer multiple time follows unspecified behavior.
     * @warning If you are passing disposable to ctor, then state of this disposable would be used used (if empty disposable or disposed -> observer is disposed by default)
     * @warning It is expected that members of this observer are called in a SERIAL manner. This means no parallel or concurrent calls are allowed—only serial calls or those guarded under a lock.
     *
     * @tparam Type of value this observer can handle
     * @tparam Strategy used to provide logic over observer's callbacks
     *
     * @ingroup observers
     */
    template<constraint::decayed_type Type, constraint::observer_strategy<Type> Strategy>
    class observer;

    template<constraint::decayed_type Type, constraint::observer_strategy<Type> Strategy>
    class observer final : public details::observer_impl<Type, Strategy, details::observers::deduce_optimal_disposables_strategy_t<Strategy::preferred_disposables_mode>>
    {
    public:
        using DisposableStrategy = details::observers::deduce_optimal_disposables_strategy_t<Strategy::preferred_disposables_mode>;
        using Base               = details::observer_impl<Type, Strategy, DisposableStrategy>;

        template<typename... Args>
            requires constraint::is_constructible_from<Strategy, Args&&...>
        explicit observer(DisposableStrategy strategy, Args&&... args)
            : Base{std::move(strategy), std::forward<Args>(args)...}
        {
        }

        template<typename... Args>
            requires (constraint::is_constructible_from<Strategy, Args && ...> && !rpp::constraint::variadic_decayed_same_as<observer, Args...>)
        explicit observer(Args&&... args)
            : Base{DisposableStrategy{}, std::forward<Args>(args)...}
        {
        }

        observer(const observer&)     = delete;
        observer(observer&&) noexcept = default;

        /**
         * @brief Convert current observer to type-erased version. Useful if you need to COPY your observer or to store different observers in same container.
         */
        dynamic_observer<Type> as_dynamic() &&
        {
            return dynamic_observer<Type>{std::move(*this)};
        }
    };

    template<constraint::decayed_type Type, constraint::observer_strategy<Type> Strategy, rpp::details::observers::constraint::disposables_strategy DisposableStrategy>
    class observer<Type, details::observers::override_disposables_strategy<Strategy, DisposableStrategy>> final : public details::observer_impl<Type, Strategy, DisposableStrategy>
    {
    public:
        using Base = details::observer_impl<Type, Strategy, DisposableStrategy>;

        template<typename... Args>
            requires constraint::is_constructible_from<Strategy, Args&&...>
        explicit observer(DisposableStrategy strategy, Args&&... args)
            : Base{std::move(strategy), std::forward<Args>(args)...}
        {
        }

        template<typename... Args>
            requires (constraint::is_constructible_from<Strategy, Args && ...> && !rpp::constraint::variadic_decayed_same_as<observer, Args...>)
        explicit observer(Args&&... args)
            : Base{DisposableStrategy{}, std::forward<Args>(args)...}
        {
        }

        observer(const observer&)     = delete;
        observer(observer&&) noexcept = default;

        /**
         * @brief Convert current observer to type-erased version. Useful if you need to COPY your observer or to store different observers in same container.
         */
        dynamic_observer<Type> as_dynamic() &&
        {
            return dynamic_observer<Type>{std::move(*this)};
        }
    };

    template<constraint::decayed_type Type>
    class observer<Type, rpp::details::observers::dynamic_strategy<Type>>
        : public details::observer_impl<Type, rpp::details::observers::dynamic_strategy<Type>, details::observers::none_disposables_strategy>
    {
    public:
        template<constraint::observer_strategy<Type> TStrategy>
            requires (!std::same_as<TStrategy, rpp::details::observers::dynamic_strategy<Type>>)
        observer(observer<Type, TStrategy>&& other)
            : details::observer_impl<Type, rpp::details::observers::dynamic_strategy<Type>, details::observers::none_disposables_strategy>{details::observers::none_disposables_strategy{}, std::move(other)}
        {
        }

        dynamic_observer<Type> as_dynamic() &&
        {
            return dynamic_observer<Type>{std::move(*this)};
        }

        dynamic_observer<Type> as_dynamic() &
        {
            return dynamic_observer<Type>{*this};
        }
    };


} // namespace rpp
