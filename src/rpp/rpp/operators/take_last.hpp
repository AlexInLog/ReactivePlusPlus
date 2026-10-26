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
#include <rpp/operators/details/strategy.hpp>

#include <cstddef>

namespace rpp::operators::details
{
    template<rpp::constraint::observer TObserver>
    class take_last_observer_strategy
    {
    public:
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        take_last_observer_strategy(TObserver&& observer, size_t count)
            : m_observer{std::move(observer)}
        {
            m_data.reserve(count);
        }

        template<typename T>
        void on_next(T&& v) const
        {
            // handle case "count==0"
            if (!m_data.capacity())
                return;

            // handle case "count==0"
            if (m_data.size() < m_data.capacity())
            {
                m_data.push_back(std::forward<T>(v));
            }
            else
            {
                m_data[m_current_end] = std::forward<T>(v);
                m_current_end         = get_next(m_current_end);
            }
        }

        void on_error(const std::exception_ptr& err) const { m_observer.on_error(err); }

        void on_completed() const
        {
            for (size_t i = 0; i < m_data.size(); ++i)
            {
                m_observer.on_next(std::move(m_data[m_current_end]));
                m_current_end = get_next(m_current_end);
            }

            m_observer.on_completed();
        }

        void set_upstream(const disposable_wrapper& d) { m_observer.set_upstream(d); }

        bool is_disposed() const { return m_observer.is_disposed(); }

    private:
        size_t get_next(size_t pos) const
        {
            return ++pos >= m_data.size() ? 0 : pos;
        }


    private:
        RPP_NO_UNIQUE_ADDRESS TObserver                                     m_observer;
        mutable std::vector<rpp::utils::extract_observer_type_t<TObserver>> m_data{};
        mutable size_t                                                      m_current_end{};
    };

    struct take_last_t : lift_operator<take_last_t, size_t>
    {
        using lift_operator<take_last_t, size_t>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            using result_type = T;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = take_last_observer_strategy<TObserver>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = Prev;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Emit only last `count` items provided by observable, then send `on_completed`
     *
     * @marble take_last
     {
         source observable       : +--1-2-3-4-5-6-|
         operator "take_last(3)" : +--------------456|
     }
     *
     * @details Actually this operator has buffer of requested size inside, keeps last `count` values and emit stored values on `on_completed`
     *
     * @param count amount of last items to be emitted
     * @note `#include <rpp/operators/take_last.hpp>`
     *
     * @par Example
     * @snippet take_last.cpp take_last
     *
     * @ingroup filtering_operators
     * @see https://reactivex.io/documentation/operators/takelast.html
     */
    inline auto take_last(size_t count)
    {
        return details::take_last_t{count};
    }
} // namespace rpp::operators
