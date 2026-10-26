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
#include <rpp/disposables/refcount_disposable.hpp>
#include <rpp/observers/observer.hpp>
#include <rpp/subjects/details/subject_on_subscribe.hpp>
#include <rpp/subjects/details/subject_state.hpp>

#include <memory>

namespace rpp::operators::details
{
    template<rpp::constraint::decayed_type Type>
    class forwarding_subject
    {
        struct observer_strategy
        {
            static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

            std::shared_ptr<subjects::details::subject_state<Type, false>> state{};

            void set_upstream(const disposable_wrapper& d) const noexcept { state->add(d); }

            bool is_disposed() const noexcept { return state->is_disposed(); }

            void on_next(const Type& v) const { state->on_next(v); }

            void on_error(const std::exception_ptr& err) const { state->on_error(err); }

            void on_completed() const { state->on_completed(); }
        };

    public:
        using optimal_disposables_strategy = typename subjects::details::subject_state<Type, false>::optimal_disposables_strategy::template add<1>;

        explicit forwarding_subject(disposable_wrapper_impl<rpp::refcount_disposable> refcount)
            : m_refcount{std::move(refcount)}
        {
        }

        auto get_observer() const
        {
            return rpp::observer<Type, observer_strategy>{m_state.lock()};
        }

        auto get_observable() const
        {
            return subjects::details::create_subject_on_subscribe_observable<Type, optimal_disposables_strategy>([state = m_state.as_weak(), refcount = m_refcount]<rpp::constraint::observer_of_type<Type> TObs>(TObs&& observer) {
                if (const auto locked_state = state.lock())
                {
                    if (const auto locked = refcount.lock())
                        observer.set_upstream(locked->add_ref());
                    locked_state->on_subscribe(std::forward<TObs>(observer));
                }
            });
        }

        rpp::composite_disposable_wrapper get_disposable() const
        {
            return m_state.as_weak();
        }

    private:
        disposable_wrapper_impl<rpp::refcount_disposable>                      m_refcount;
        disposable_wrapper_impl<subjects::details::subject_state<Type, false>> m_state = disposable_wrapper_impl<subjects::details::subject_state<Type, false>>::make();
    };
} // namespace rpp::operators::details
