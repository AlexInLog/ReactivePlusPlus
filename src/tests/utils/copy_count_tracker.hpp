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

#include <doctest/doctest.h>

#include <rpp/sources/create.hpp>
#include <rpp/utils/functors.hpp>

#include <memory>

class copy_count_tracker
{
public:
    copy_count_tracker()
        : m_state(std::make_shared<state>())
    {
    }

    copy_count_tracker(const copy_count_tracker& other)
        : m_state{other.m_state}
    {
        ++m_state->copy_count;
    }

    copy_count_tracker(copy_count_tracker&& other) noexcept
        : m_state{other.m_state} // NOLINT(performance-move-constructor-init)
    {
        ++m_state->move_count;
    }

    copy_count_tracker& operator=(const copy_count_tracker& other)
    {
        if (this == &other)
            return *this;
        m_state = other.m_state;
        ++m_state->copy_count;
        return *this;
    }

    copy_count_tracker& operator=(copy_count_tracker&& other) noexcept
    {
        if (this == &other)
            return *this;
        m_state = other.m_state;
        ++m_state->move_count;
        return *this;
    }

    bool operator==(const copy_count_tracker& other) const
    {
        return m_state == other.m_state;
    }

    bool operator!=(const copy_count_tracker& other) const { return !(*this == other); }

    auto get_observable(size_t count = 1)
    {
        return rpp::source::create<copy_count_tracker>([this, count](const auto& sub) {
            for (size_t i = 0; i < count && !sub.is_disposed(); ++i)
                sub.on_next(*this);
            sub.on_completed();
        });
    }

    auto get_observable_for_move(size_t count = 1)
    {
        return rpp::source::create<copy_count_tracker>([this, count](const auto& sub) {
            for (size_t i = 0; i < count && !sub.is_disposed(); ++i)
                sub.on_next(std::move(*this));
            sub.on_completed();
        });
    }

    struct state
    {
        int copy_count = 0;
        int move_count = 0;
    };

    struct test_expectations
    {
        state send_by_copy{};
        state send_by_move{};
    };


    static void test_operator(auto&& op, test_expectations expectations, size_t count = 1)
    {
        copy_count_tracker tracker{};
        SUBCASE("send value by copy")
        {
            tracker.get_observable(count) | op | rpp::ops::subscribe(rpp::utils::empty_function_any_by_lvalue_t{});
            CHECK(tracker.get_copy_count() == expectations.send_by_copy.copy_count);
            CHECK(tracker.get_move_count() == expectations.send_by_copy.move_count);
        }

        SUBCASE("send value by move")
        {
            tracker.get_observable_for_move(count) | op | rpp::ops::subscribe(rpp::utils::empty_function_any_by_lvalue_t{});
            CHECK(tracker.get_copy_count() == expectations.send_by_move.copy_count);
            CHECK(tracker.get_move_count() == expectations.send_by_move.move_count);
        }
    }

    int get_copy_count() const { return m_state->copy_count; }
    int get_move_count() const { return m_state->move_count; }

private:
    std::shared_ptr<state> m_state;
};

namespace std
{
    // Make copy_count_tracker hashable for distinct operator
    template<>
    struct hash<copy_count_tracker>
    {
        size_t operator()(const copy_count_tracker& tracker) const noexcept
        {
            return std::hash<int>{}(tracker.get_copy_count())
                 ^ (std::hash<int>{}(tracker.get_move_count()) << 1);
        }
    };
} // namespace std
