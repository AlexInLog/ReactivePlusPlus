//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2022 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/schedulers/fwd.hpp>
#include <rpp/subjects/fwd.hpp>

#include <rpp/disposables/disposable_wrapper.hpp>
#include <rpp/observers/observer.hpp>
#include <rpp/subjects/details/subject_on_subscribe.hpp>
#include <rpp/subjects/details/subject_state.hpp>

#include <utility>

namespace rpp::subjects::details
{
    template<rpp::constraint::decayed_type Type, bool Serialized>
    class behavior_subject_base
    {
        class behavior_state final : public subject_state<Type, Serialized>
        {
        public:
            behavior_state(const Type& v)
                : m_value{v}
            {
            }
            behavior_state(Type&& v)
                : m_value{std::move(v)}
            {
            }

            rpp::utils::pointer_under_lock<Type> get_value() { return rpp::utils::pointer_under_lock<Type>{m_value}; }

        private:
            rpp::utils::value_with_mutex<Type> m_value{};
        };

        struct observer_strategy
        {
            static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

            std::shared_ptr<behavior_state> state;

            void set_upstream(const disposable_wrapper& d) const noexcept { state->add(d); }

            bool is_disposed() const noexcept { return state->is_disposed(); }

            void on_next(const Type& v) const
            {
                *state->get_value() = v;
                state->on_next(v);
            }

            void on_error(const std::exception_ptr& err) const { state->on_error(err); }

            void on_completed() const { state->on_completed(); }
        };

    public:
        using optimal_disposables_strategy = typename details::subject_state<Type, Serialized>::optimal_disposables_strategy;

        explicit behavior_subject_base(const Type& value)
            : m_state{disposable_wrapper_impl<behavior_state>::make(value)}
        {
        }

        explicit behavior_subject_base(Type&& value)
            : m_state{disposable_wrapper_impl<behavior_state>::make(std::move(value))}
        {
        }

        auto get_observer() const
        {
            return rpp::observer<Type, observer_strategy>{m_state.lock()};
        }

        auto get_observable() const
        {
            return create_subject_on_subscribe_observable<Type, optimal_disposables_strategy>([state = m_state]<rpp::constraint::observer_of_type<Type> TObs>(TObs&& observer) {
                const auto locked = state.lock();
                if (!locked->is_disposed())
                {
                    auto v = *locked->get_value();
                    observer.on_next(std::move(v));
                }
                locked->on_subscribe(std::forward<TObs>(observer));
            });
        }

        rpp::disposable_wrapper get_disposable() const
        {
            return m_state;
        }

        Type get_value() const
        {
            return *m_state.lock()->get_value();
        }


    private:
        disposable_wrapper_impl<behavior_state> m_state;
    };
} // namespace rpp::subjects::details

namespace rpp::subjects
{
    /**
     * @brief Same as rpp::subjects::publish_subject but keeps last value (or default) and emits it to newly subscribed observer
     *
     * @tparam Type value provided by this subject
     *
     * @ingroup subjects
     * @see https://reactivex.io/documentation/subject.html
     */
    template<rpp::constraint::decayed_type Type>
    class behavior_subject final : public details::behavior_subject_base<Type, false>
    {
    public:
        using details::behavior_subject_base<Type, false>::behavior_subject_base;
    };

    /**
     * @brief Same as rpp::subjects::behavior_subject but on_next/on_error/on_completed calls are serialized via mutex.
     * @details When you are using ordinary rpp::subjects::behavior_subject, then you must take care not to call its on_next method (or its other on methods) in async way.
     *
     * @ingroup subjects
     * @see https://reactivex.io/documentation/subject.html
     */
    template<rpp::constraint::decayed_type Type>
    class serialized_behavior_subject final : public details::behavior_subject_base<Type, true>
    {
    public:
        using details::behavior_subject_base<Type, true>::behavior_subject_base;
    };
} // namespace rpp::subjects
