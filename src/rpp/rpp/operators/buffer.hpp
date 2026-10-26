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
    class buffer_observer_strategy
    {
        using container  = rpp::utils::extract_observer_type_t<TObserver>;
        using value_type = typename container::value_type;
        static_assert(std::same_as<container, std::vector<value_type>>);

    public:
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        buffer_observer_strategy(TObserver&& observer, size_t count)
            : m_observer{std::move(observer)}
        {
            m_bucket.reserve(std::max(size_t{1}, count));
        }

        template<typename T>
        void on_next(T&& v) const
        {
            m_bucket.push_back(std::forward<T>(v));
            if (m_bucket.size() == m_bucket.capacity())
            {
                const auto capacity = m_bucket.capacity();
                m_observer.on_next(std::move(m_bucket));

                m_bucket.clear();
                m_bucket.reserve(capacity);
            }
        }

        void on_error(const std::exception_ptr& err) const { m_observer.on_error(err); }

        void on_completed() const
        {
            if (!m_bucket.empty())
                m_observer.on_next(std::move(m_bucket));
            m_observer.on_completed();
        }

        void set_upstream(const disposable_wrapper& d) { m_observer.set_upstream(d); }

        bool is_disposed() const { return m_observer.is_disposed(); }

    private:
        RPP_NO_UNIQUE_ADDRESS TObserver m_observer;
        mutable std::vector<value_type> m_bucket;
    };

    struct buffer_t : lift_operator<buffer_t, size_t>
    {
        using lift_operator<buffer_t, size_t>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            using result_type = std::vector<T>;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = buffer_observer_strategy<TObserver>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = Prev;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Periodically gather emissions emitted by an original Observable into bundles and emit these bundles rather than emitting
     * the items one at a time
     *
     * @marble buffer
         {
             source observable    : +-1-2-3-|
             operator "buffer(2)" : +---{1,2}-{3}-|
         }
     *
     * @details The resulting bundle is `std::vector<Type>` of requested size. Actually it is similar to `window()` operator, but it emits vectors instead of observables.
     *
     * @param count number of items being bundled.
     * @note `#include <rpp/operators/buffer.hpp>`
     *
     * @par Example:
     * @snippet buffer.cpp buffer
     *
     * @ingroup transforming_operators
     * @see https://reactivex.io/documentation/operators/buffer.html
     */
    inline auto buffer(size_t count)
    {
        return details::buffer_t{count};
    }
} // namespace rpp::operators
