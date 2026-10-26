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
#include <rpp/utils/exceptions.hpp>

#include <cstddef>

namespace rpp::operators::details
{
    template<rpp::constraint::observer TObserver>
    struct element_at_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        RPP_NO_UNIQUE_ADDRESS TObserver observer;
        mutable size_t                  count;

        template<typename T>
        void on_next(T&& v) const
        {
            if (count)
            {
                --count;
            }

            if (!count)
            {
                observer.on_next(std::forward<T>(v));
                observer.on_completed();
            }
        }

        void on_error(const std::exception_ptr& err) const { observer.on_error(err); }

        void on_completed() const
        {
            if (count)
            {
                observer.on_error(std::make_exception_ptr(utils::out_of_range{"index is out of bounds"}));
            }
            else
            {
                observer.on_completed();
            }
        }

        void set_upstream(const disposable_wrapper& d) { observer.set_upstream(d); }

        bool is_disposed() const { return observer.is_disposed(); }
    };

    struct element_at_t : lift_operator<element_at_t, size_t>
    {
        using lift_operator<element_at_t, size_t>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            using result_type = T;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = element_at_observer_strategy<TObserver>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = Prev;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Emit item located at specified `index` location in the sequence of items emitted by the source observable
     *
     * @marble element_at
     {
         source observable        : +--1-2--3-4---|
         operator "element_at(2)" : +-------3|
     }
     * @details If source observable completes without emitting at least `index` + 1 items, observable emits an error
     *
     * @param index index of the item to return
     * @note `#include <rpp/operators/element_at.hpp>`
     *
     * @ingroup filtering_operators
     * @see https://reactivex.io/documentation/operators/elementat.html
     */
    inline auto element_at(size_t index)
    {
        return details::element_at_t{index + 1};
    }
} // namespace rpp::operators
