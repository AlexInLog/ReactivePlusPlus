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

#include <rpp/disposables/fwd.hpp>

#include <rpp/observers/observer.hpp>

#include <vector>

template<typename Type>
class mock_observer_strategy final
{
public:
    static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::Auto;

    explicit mock_observer_strategy(bool copy_values = true)
        : m_state{std::make_shared<state>(copy_values)}
    {
    }

    void on_next(const Type& v) const noexcept
    {
        ++m_state->m_on_next_const_ref_count;
        if (m_state->m_copy_values)
            m_state->vals.push_back(v);
    }

    void on_next(Type&& v) const noexcept
    {
        ++m_state->m_on_next_move_count;
        if (m_state->m_copy_values)
            m_state->vals.push_back(std::move(v));
    }

    void on_error(const std::exception_ptr&) const noexcept { ++m_state->m_on_error_count; }
    void on_completed() const noexcept { ++m_state->m_on_completed_count; }

    static bool is_disposed() noexcept { return false; }
    static void set_upstream(const rpp::disposable_wrapper&) noexcept { }

    size_t get_total_on_next_count() const { return m_state->m_on_next_const_ref_count + m_state->m_on_next_move_count; }
    size_t get_on_next_const_ref_count() const { return m_state->m_on_next_const_ref_count; }
    size_t get_on_next_move_count() const { return m_state->m_on_next_move_count; }
    size_t get_on_error_count() const { return m_state->m_on_error_count; }
    size_t get_on_completed_count() const { return m_state->m_on_completed_count; }

    std::vector<Type> get_received_values() const { return m_state->vals; }

    auto get_observer() const { return rpp::observer<Type, mock_observer_strategy<Type>>{*this}; }
    auto get_observer(rpp::composite_disposable_wrapper d) const { return rpp::observer_with_external_disposable<Type, mock_observer_strategy<Type>>{std::move(d), *this}; }

private:
    struct state
    {
        explicit state(bool copy_values)
            : m_copy_values{copy_values}
        {
        }

        bool   m_copy_values             = true;
        size_t m_on_next_const_ref_count = 0;
        size_t m_on_next_move_count      = 0;
        size_t m_on_error_count          = 0;
        size_t m_on_completed_count      = 0;

        std::vector<Type> vals{};
    };

    std::shared_ptr<state> m_state{};
};
