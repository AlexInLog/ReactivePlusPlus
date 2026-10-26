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
#include <rpp/disposables/refcount_disposable.hpp>
#include <rpp/operators/details/forwarding_subject.hpp>
#include <rpp/operators/details/strategy.hpp>

#include <cstddef>

namespace rpp
{
    template<constraint::decayed_type Type>
    using window_observable = decltype(std::declval<rpp::operators::details::forwarding_subject<Type>>().get_observable());
} // namespace rpp
namespace rpp::operators::details
{
    template<rpp::constraint::observer TObserver>
    class window_observer_strategy
    {
        using Observable = rpp::utils::extract_observer_type_t<TObserver>;
        using value_type = rpp::utils::extract_observable_type_t<Observable>;
        using Subject    = forwarding_subject<value_type>;

        static_assert(std::same_as<Observable, decltype(std::declval<Subject>().get_observable())>);

    public:
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        window_observer_strategy(TObserver&& observer, size_t count)
            : m_observer{std::move(observer)}
            , m_window_size{std::max(size_t{1}, count)}
        {
            m_observer.set_upstream(m_disposable->add_ref());
        }

        template<typename T>
        void on_next(T&& v) const
        {
            // need to send new subject due to NEW item appeared (we avoid sending new subjects if no any new items)
            if (m_items_in_current_window == m_window_size)
            {
                Subject subject{m_disposable->wrapper_from_this()};
                m_subject_data.emplace(subject.get_observer(), subject.get_disposable());
                m_disposable->add(m_subject_data->disposable);
                m_observer.on_next(subject.get_observable());
                m_items_in_current_window = 0;
            }

            m_subject_data->observer.on_next(std::forward<T>(v));

            // cleanup current subject, but don't send due to wait for new value
            if (++m_items_in_current_window == m_window_size)
            {
                m_subject_data->observer.on_completed();
                m_disposable->remove(m_subject_data->disposable);
                m_subject_data.reset();
            }
        }

        void on_error(const std::exception_ptr& err) const
        {
            if (m_subject_data)
                m_subject_data->observer.on_error(err);
            m_observer.on_error(err);
        }

        void on_completed() const
        {
            if (m_subject_data)
                m_subject_data->observer.on_completed();
            m_observer.on_completed();
        }

        void set_upstream(const disposable_wrapper& d) const { m_disposable->add(d); }

        bool is_disposed() const { return m_disposable->is_disposed(); }

    private:
        std::shared_ptr<refcount_disposable> m_disposable = disposable_wrapper_impl<refcount_disposable>::make().lock();
        RPP_NO_UNIQUE_ADDRESS TObserver      m_observer;

        struct subject_data
        {
            using TObs = decltype(std::declval<Subject>().get_observer());

            subject_data(TObs&& obs, rpp::disposable_wrapper&& d)
                : observer{std::move(obs)}
                , disposable{std::move(d)}
            {
            }

            TObs                    observer;
            rpp::disposable_wrapper disposable;
        };

        mutable std::optional<subject_data> m_subject_data;
        const size_t                        m_window_size;
        mutable size_t                      m_items_in_current_window = m_window_size;
    };

    struct window_t : lift_operator<window_t, size_t>
    {
        using lift_operator<window_t, size_t>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            using result_type = window_observable<T>;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = window_observer_strategy<TObserver>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<1>;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Subdivide original observable into sub-observables (window observables) and emit sub-observables of items instead of original items
     *
     * @marble window
       {
           source observable    :  +-1-2-3-4-5-|

           operator "window(2)" :
                               {
                                   .+1-2|
                                   .....+3-4|
                                   .........+5-|
                               }
       }
     *
     * @details Actually it is similar to `buffer` but it emits observable instead of container.
     *
     * @param count amount of items which every observable would have
     *
     * @note `#include <rpp/operators/window.hpp>`
     *
     * @par Example
     * @snippet window.cpp window
     *
     * @ingroup transforming_operators
     * @see https://reactivex.io/documentation/operators/window.html
     */
    inline auto window(size_t count)
    {
        return details::window_t{count};
    }
} // namespace rpp::operators
