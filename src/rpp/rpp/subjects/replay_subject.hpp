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

#include <deque>
#include <utility>

namespace rpp::subjects::details
{
    template<rpp::constraint::decayed_type Type, bool Serialized>
    class replay_subject_base
    {
        struct replay_state final : public subject_state<Type, Serialized>
        {
            replay_state(size_t limit = std::numeric_limits<size_t>::max(), rpp::schedulers::duration duration_limit = std::numeric_limits<rpp::schedulers::duration>::max())
                : m_limit(limit)
                , m_duration_limit(duration_limit)
            {
            }

            void add_value(const Type& v)
            {
                std::unique_lock lock{m_values_mutex};
                while (m_values.size() >= m_limit)
                    m_values.pop_front();

                m_values.emplace_back(v, deduce_timepoint());
            }

            struct value_with_time
            {
                value_with_time(const Type& v, rpp::schedulers::clock_type::time_point timepoint)
                    : value{v}
                    , timepoint{timepoint}
                {
                }

                Type                                    value;
                rpp::schedulers::clock_type::time_point timepoint;
            };


            std::deque<value_with_time> get_actual_values()
            {
                std::unique_lock lock{m_values_mutex};
                deduce_timepoint();
                return m_values;
            }

        private:
            rpp::schedulers::clock_type::time_point deduce_timepoint()
            {
                if (std::numeric_limits<rpp::schedulers::duration>::max() == m_duration_limit)
                    return rpp::schedulers::clock_type::time_point{};

                auto now = rpp::schedulers::clock_type::now();
                while (!m_values.empty() && (now - m_values.front().timepoint > m_duration_limit))
                    m_values.pop_front();
                return now;
            }

        private:
            std::mutex                  m_values_mutex{};
            std::deque<value_with_time> m_values{};

            const size_t                    m_limit;
            const rpp::schedulers::duration m_duration_limit;
        };

        struct observer_strategy
        {
            static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

            std::shared_ptr<replay_state> state;

            void set_upstream(const disposable_wrapper& d) const noexcept { state->add(d); }

            bool is_disposed() const noexcept { return state->is_disposed(); }

            void on_next(const Type& v) const
            {
                state->add_value(v);
                state->on_next(v);
            }

            void on_error(const std::exception_ptr& err) const { state->on_error(err); }

            void on_completed() const { state->on_completed(); }
        };

    public:
        using optimal_disposables_strategy = typename details::subject_state<Type, Serialized>::optimal_disposables_strategy;

        replay_subject_base()
            : m_state{disposable_wrapper_impl<replay_state>::make()}
        {
        }

        replay_subject_base(size_t count)
            : m_state{disposable_wrapper_impl<replay_state>::make(std::max<size_t>(1, count))}
        {
        }

        replay_subject_base(size_t count, rpp::schedulers::duration duration)
            : m_state{disposable_wrapper_impl<replay_state>::make(std::max<size_t>(1, count), duration)}
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
                for (auto&& value : locked->get_actual_values())
                    observer.on_next(std::move(value.value));
                locked->on_subscribe(std::forward<TObs>(observer));
            });
        }

        rpp::disposable_wrapper get_disposable() const
        {
            return m_state;
        }

    private:
        disposable_wrapper_impl<replay_state> m_state;
    };
} // namespace rpp::subjects::details

namespace rpp::subjects
{
    /**
     * @brief Same as rpp::subjects::publish_subject but send all earlier emitted values to any new observers.
     *
     * @param count maximum element count of the replay buffer (optional)
     * @param duration maximum time length the replay buffer (optional)
     *
     * @tparam Type value provided by this subject
     *
     * @ingroup subjects
     * @see https://reactivex.io/documentation/subject.html
     */
    template<rpp::constraint::decayed_type Type>
    class replay_subject final : public details::replay_subject_base<Type, false>
    {
    public:
        using details::replay_subject_base<Type, false>::replay_subject_base;
    };

    /**
     * @brief Same as rpp::subjects::replay_subject but on_next/on_error/on_completed calls are serialized via mutex.
     * @details When you are using ordinary rpp::subjects::replay_subject, then you must take care not to call its on_next method (or its other on methods) in async way.
     *
     * @ingroup subjects
     * @see https://reactivex.io/documentation/subject.html
     */
    template<rpp::constraint::decayed_type Type>
    class serialized_replay_subject final : public details::replay_subject_base<Type, true>
    {
    public:
        using details::replay_subject_base<Type, true>::replay_subject_base;
    };
} // namespace rpp::subjects
