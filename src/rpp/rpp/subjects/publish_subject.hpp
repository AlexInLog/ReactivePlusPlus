//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2022 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/subjects/fwd.hpp>

#include <rpp/disposables/disposable_wrapper.hpp>
#include <rpp/observers/observer.hpp>
#include <rpp/subjects/details/subject_on_subscribe.hpp>
#include <rpp/subjects/details/subject_state.hpp>

namespace rpp::subjects::details
{
    template<rpp::constraint::decayed_type Type, bool Serialized>
    class publish_subject_base
    {
        struct observer_strategy
        {
            static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

            std::shared_ptr<details::subject_state<Type, Serialized>> state{};

            void set_upstream(const disposable_wrapper& d) const noexcept { state->add(d); }

            bool is_disposed() const noexcept { return state->is_disposed(); }

            void on_next(const Type& v) const { state->on_next(v); }

            void on_error(const std::exception_ptr& err) const { state->on_error(err); }

            void on_completed() const { state->on_completed(); }
        };

    public:
        using optimal_disposables_strategy = typename details::subject_state<Type, Serialized>::optimal_disposables_strategy;

        publish_subject_base() = default;

        auto get_observer() const
        {
            return rpp::observer<Type, observer_strategy>{m_state.lock()};
        }

        auto get_observable() const
        {
            return create_subject_on_subscribe_observable<Type, optimal_disposables_strategy>([state = m_state]<rpp::constraint::observer_of_type<Type> TObs>(TObs&& observer) { state.lock()->on_subscribe(std::forward<TObs>(observer)); });
        }

        rpp::disposable_wrapper get_disposable() const
        {
            return m_state;
        }

    private:
        disposable_wrapper_impl<details::subject_state<Type, Serialized>> m_state = disposable_wrapper_impl<subject_state<Type, Serialized>>::make();
    };
} // namespace rpp::subjects::details
namespace rpp::subjects
{
    /**
     * @brief Subject which just multicasts values to observers subscribed on it. It contains two parts: observer and observable at the same time.
     *
     * @details Each observer obtains only values which emitted after corresponding subscribe. on_error/on_completer/unsubscribe cached and provided to new observers if any
     *
     * @warning this subject is not synchronized/serialized! It means, that expected to call callbacks of observer in the serialized way to follow observable contract: "Observables must issue notifications to observers serially (not in parallel).". If you are not sure or need extra serialization, please, use serialized_publish_subject.
     *
     * @tparam Type value provided by this subject
     *
     * @ingroup subjects
     * @see https://reactivex.io/documentation/subject.html
     */
    template<rpp::constraint::decayed_type Type>
    class publish_subject final : public details::publish_subject_base<Type, false>
    {
    public:
        using details::publish_subject_base<Type, false>::publish_subject_base;
    };

    /**
     * @brief Serialized version of rpp::subjects::publish_subject
     * @details When you are using ordinary rpp::subjects::publish_subject, then you must take care not to call its on_next method (or its other on methods) in async way.
     *
     * @ingroup subjects
     * @see https://reactivex.io/documentation/subject.html
     */
    template<rpp::constraint::decayed_type Type>
    class serialized_publish_subject final : public details::publish_subject_base<Type, true>
    {
    public:
        using details::publish_subject_base<Type, true>::publish_subject_base;
    };
} // namespace rpp::subjects
