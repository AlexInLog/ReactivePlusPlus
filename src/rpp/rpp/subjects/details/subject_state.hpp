//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2022 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/observables/fwd.hpp>

#include <rpp/disposables/callback_disposable.hpp>
#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/disposables/disposable_wrapper.hpp>
#include <rpp/observers/dynamic_observer.hpp>
#include <rpp/utils/constraints.hpp>
#include <rpp/utils/functors.hpp>
#include <rpp/utils/utils.hpp>

#include <algorithm>
#include <list>
#include <memory>
#include <mutex>
#include <variant>

namespace rpp::subjects::details
{
    struct completed
    {
    };

    struct disposed
    {
    };

    template<rpp::constraint::decayed_type Type, bool Serialized>
    class subject_state : public composite_disposable
        , public rpp::details::enable_wrapper_from_this<subject_state<Type, Serialized>>
    {
        template<rpp::constraint::observer TObs>
        class disposable_with_observer : public rpp::details::observers::type_erased_observer<TObs>
            , public rpp::details::base_disposable
        {
        public:
            disposable_with_observer(TObs&& observer, std::weak_ptr<subject_state> state)
                : rpp::details::observers::type_erased_observer<TObs>{std::move(observer)}
                , m_state{std::move(state)}
            {
            }

        private:
            void base_dispose_impl(interface_disposable::Mode) noexcept override
            {
                if (const auto shared = m_state.lock())
                {
                    std::unique_lock lock{shared->m_mutex};
                    process_state_unsafe(shared->m_state,
                                         [&](const shared_observers& observers) {
                                             shared->m_state = cleanup_observers(observers, this);
                                         });
                }
            }

            std::weak_ptr<subject_state> m_state{};
        };

        using observer         = std::shared_ptr<rpp::details::observers::observer_vtable<Type>>;
        using observers        = std::list<observer>;
        using shared_observers = std::shared_ptr<observers>;
        using state_t          = std::variant<shared_observers, std::exception_ptr, completed, disposed>;

    public:
        using optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<1>;

        subject_state() = default;

        template<rpp::constraint::observer_of_type<Type> TObs>
        void on_subscribe(TObs&& observer)
        {
            std::unique_lock lock{m_mutex};
            process_state_unsafe(
                m_state,
                [&](const shared_observers& observers) {
                    auto d   = disposable_wrapper_impl<disposable_with_observer<std::decay_t<TObs>>>::make(std::forward<TObs>(observer), this->wrapper_from_this().lock());
                    auto ptr = d.lock();
                    if (!observers)
                    {
                        auto new_observers = std::make_shared<subject_state::observers>();
                        new_observers->emplace_back(ptr);
                        m_state = std::move(new_observers);
                    }
                    else
                    {
                        observers->emplace_back(ptr);
                    }

                    lock.unlock();
                    ptr->set_upstream(d.as_weak());
                },
                [&](const std::exception_ptr& err) {
                    lock.unlock();
                    observer.on_error(err);
                },
                [&](completed) {
                    lock.unlock();
                    observer.on_completed();
                });
        }

        void on_next(const Type& v)
        {
            std::unique_lock observers_lock{m_mutex};
            process_state_unsafe(m_state, [&](shared_observers observers) {
                if (!observers)
                    return;

                auto       itr  = observers->cbegin();
                const auto size = observers->size();

                observers_lock.unlock();

                std::lock_guard lock{m_serialized_mutex};
                for (size_t i = 0; i < size; ++i)
                {
                    (*(itr++))->on_next(v);
                }
            });
        }

        void on_error(const std::exception_ptr& err)
        {
            {
                std::lock_guard lock{m_serialized_mutex};
                if (const auto observers = exchange_observers_under_lock_if_there(err))
                    rpp::utils::for_each(*observers, [&](const observer& obs) { obs->on_error(err); });
            }
            dispose();
        }

        void on_completed()
        {
            {
                std::lock_guard lock{m_serialized_mutex};
                if (const auto observers = exchange_observers_under_lock_if_there(completed{}))
                    rpp::utils::for_each(*observers, [](const observer& obs) { obs->on_completed(); });
            }
            dispose();
        }

    private:
        void composite_dispose_impl(interface_disposable::Mode) noexcept override
        {
            exchange_observers_under_lock_if_there(disposed{});
        }

        static shared_observers cleanup_observers(const shared_observers& current_subs, const rpp::details::observers::observer_vtable<Type>* to_delete)
        {
            auto subs = std::make_shared<observers>();
            if (current_subs)
            {
                std::copy_if(current_subs->cbegin(),
                             current_subs->cend(),
                             std::back_inserter(*subs),
                             [&to_delete](const observer& obs) {
                                 return to_delete != obs.get();
                             });
            }
            return subs;
        }

        static auto process_state_unsafe(const state_t& state, const auto&... actions)
        {
            return std::visit(rpp::utils::overloaded{actions..., rpp::utils::empty_function_any_t{}}, state);
        }

        shared_observers exchange_observers_under_lock_if_there(state_t&& new_val)
        {
            std::lock_guard lock{m_mutex};

            return process_state_unsafe(m_state, [&](shared_observers observers) {
                m_state = std::move(new_val);
                return observers; }, [](auto) { return shared_observers{}; });
        }

    private:
        state_t                                                                                  m_state;
        std::mutex                                                                               m_mutex{};
        RPP_NO_UNIQUE_ADDRESS std::conditional_t<Serialized, std::mutex, rpp::utils::none_mutex> m_serialized_mutex{};
    };
} // namespace rpp::subjects::details
