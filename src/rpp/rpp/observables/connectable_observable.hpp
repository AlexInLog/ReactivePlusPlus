//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/subjects/fwd.hpp>

#include <rpp/disposables/refcount_disposable.hpp>
#include <rpp/observables/observable.hpp>

#include <mutex>

namespace rpp::details
{
    template<typename T>
    struct ref_count_on_subscribe_t;

    template<rpp::constraint::observable OriginalObservable, rpp::constraint::subject Subject>
    struct ref_count_on_subscribe_t<rpp::connectable_observable<OriginalObservable, Subject>>
    {
        rpp::connectable_observable<OriginalObservable, Subject> original_observable;

        struct state_t
        {
            std::mutex                                        mutex{};
            disposable_wrapper_impl<rpp::refcount_disposable> disposable = disposable_wrapper_impl<rpp::refcount_disposable>::empty();
        };

        std::shared_ptr<state_t> m_state = std::make_shared<state_t>();

        using value_type                   = rpp::utils::extract_observable_type_t<OriginalObservable>;
        using optimal_disposables_strategy = typename rpp::connectable_observable<OriginalObservable, Subject>::optimal_disposables_strategy::template add<1>;

        template<constraint::observer_strategy<value_type> Strategy>
        void subscribe(observer<value_type, Strategy>&& obs) const
        {
            auto [disposable, upstream] = on_subscribe();

            obs.set_upstream(disposable);
            original_observable.subscribe(std::move(obs));
            if (!upstream.is_disposed())
                original_observable.connect(std::move(upstream));
        }

    private:
        std::pair<rpp::disposable_wrapper, rpp::composite_disposable_wrapper> on_subscribe() const
        {
            std::unique_lock lock(m_state->mutex);
            if (!m_state->disposable.is_disposed())
                return {m_state->disposable.lock()->add_ref(), composite_disposable_wrapper::empty()};

            m_state->disposable = disposable_wrapper_impl<rpp::refcount_disposable>::make();
            return {m_state->disposable.lock()->add_ref(), m_state->disposable};
        }
    };
} // namespace rpp::details

namespace rpp
{
    /**
     * @brief Extension over raw observable with ability to be manually connected at any time or ref_counting (sharing same observable between multiple observers)
     *
     * @ingroup observables
     */
    template<rpp::constraint::observable OriginalObservable, typename Subject>
    class connectable_observable : public decltype(std::declval<Subject>().get_observable())
    {
        using base = decltype(std::declval<Subject>().get_observable());

    public:
        static_assert(rpp::constraint::subject<Subject>);

        connectable_observable(const OriginalObservable& original_observable, const Subject& subject)
            : base{subject.get_observable()}
            , m_original_observable{original_observable}
            , m_subject{subject}
        {
        }

        connectable_observable(OriginalObservable && original_observable, const Subject& subject)
            : base{subject.get_observable()}
            , m_original_observable{std::move(original_observable)}
            , m_subject{subject}
        {
        }

        /**
         * @brief Connects to underlying observable right-now making it hot-observable
         *
         * @par Example:
         * @snippet connect.cpp connect
         *
         */
        rpp::disposable_wrapper connect(rpp::composite_disposable_wrapper wrapper = composite_disposable_wrapper::make()) const
        {
            std::unique_lock lock(m_state->mutex);

            if (m_subject.get_disposable().is_disposed())
                return rpp::disposable_wrapper::empty();

            if (!m_state->disposable.is_disposed())
                return m_state->disposable;

            m_state->disposable = wrapper;
            lock.unlock();

            m_original_observable.subscribe(wrapper, m_subject.get_observer());
            return wrapper;
        }

        /**
         * @brief Forces rpp::connectable_observable to behave like common observable
         * @details Connects rpp::connectable_observable on the first subscription and unsubscribes on last unsubscription
         *
         * @par Example
         * @snippet ref_count.cpp ref_count
         *
         * @ingroup connectable_operators
         * @see https://reactivex.io/documentation/operators/refcount.html
         */
        auto ref_count() const
        {
            return rpp::observable<rpp::utils::extract_observable_type_t<OriginalObservable>,
                                   details::ref_count_on_subscribe_t<connectable_observable<OriginalObservable, Subject>>>{*this};
        }

        template<typename Op>
        auto operator|(Op&& op) const &
        {
            if constexpr (std::invocable<std::decay_t<Op>, const connectable_observable&>)
            {
                static_assert(rpp::constraint::observable<std::invoke_result_t<std::decay_t<Op>, const connectable_observable&>>, "Result of Op should be observable");
                return std::forward<Op>(op)(*this);
            }
            else
                return static_cast<const base&>(*this) | std::forward<Op>(op);
        }

        template<typename Op>
        auto operator|(Op&& op) &&
        {
            if constexpr (std::invocable<std::decay_t<Op>, connectable_observable&&>)
            {
                static_assert(rpp::constraint::observable<std::invoke_result_t<std::decay_t<Op>, connectable_observable&&>>, "Result of Op should be observable");
                return std::forward<Op>(op)(std::move(*this));
            }
            else
                return static_cast<base&&>(*this) | std::forward<Op>(op);
        }

        template<typename Op>
        auto pipe(Op && op) const &
        {
            return *this | std::forward<Op>(op);
        }

        template<typename Op>
        auto pipe(Op && op) &&
        {
            return std::move(*this) | std::forward<Op>(op);
        }

        auto as_dynamic_connectable() const &
        {
            return rpp::dynamic_connectable_observable<Subject>{m_original_observable.as_dynamic(), m_subject};
        }
        auto as_dynamic_connectable() &&
        {
            return rpp::dynamic_connectable_observable<Subject>{std::move(m_original_observable).as_dynamic(), std::move(m_subject)};
        }

    private:
        RPP_NO_UNIQUE_ADDRESS OriginalObservable m_original_observable;
        Subject                                  m_subject;

        struct state_t
        {
            std::mutex                        mutex{};
            rpp::composite_disposable_wrapper disposable = composite_disposable_wrapper::empty();
        };

        std::shared_ptr<state_t> m_state = std::make_shared<state_t>();
    };
} // namespace rpp
