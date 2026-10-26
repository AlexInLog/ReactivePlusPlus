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

#include <rpp/sources/concat.hpp>
#include <rpp/sources/from.hpp>

namespace rpp::operators::details
{
    template<rpp::constraint::observable... TObservables>
    struct start_with_t
    {
        rpp::utils::tuple<TObservables...> observables{};

        template<rpp::constraint::observable TObservable>
        auto operator()(TObservable&& observable) const
        {
            static_assert(rpp::constraint::observables_of_same_type<TObservable, TObservables...>, "observables should be of same type");
            return observables.apply(&apply<TObservable>, std::forward<TObservable>(observable));
        }

    private:
        template<rpp::constraint::observable TObservable>
        static auto apply(TObservable&& observable, const TObservables&... observables)
        {
            return rpp::source::concat(observables..., std::forward<TObservable>(observable));
        }
    };

    template<rpp::constraint::decayed_type PackedContainer, rpp::schedulers::constraint::scheduler TScheduler>
    struct start_with_values_t
    {
        RPP_NO_UNIQUE_ADDRESS PackedContainer container;
        RPP_NO_UNIQUE_ADDRESS TScheduler      scheduler;

        template<typename... Args>
        start_with_values_t(const TScheduler& scheduler, Args&&... args)
            : container{std::forward<Args>(args)...}
            , scheduler{scheduler}
        {
        }

        template<rpp::constraint::observable TObservable>
        auto operator()(TObservable&& observable) const
        {
            static_assert(rpp::constraint::observable_of_type<TObservable, rpp::utils::iterable_value_t<PackedContainer>>, "observables should be of same type");
            return rpp::source::concat(rpp::source::from_iterable(container, scheduler), std::forward<TObservable>(observable));
        }
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Combines submissions from current observable with other observables into one but without overlapping and starting from observables provided as arguments
     * @warning If by some reason you need to interpet observables as "values", not sources of data, then use `start_with_values` instead
     *
     * @marble start_with_observable
         {
             source original_observable       : +-4--6-|
             operator "start_with(-1-2-3-|)"  : +--1-2-3--4--6-|
         }
     *
     * @details Actually it makes concat(observables_to_start_with..., current_observable) so observables from argument subscribed before current observable
     *
     * @param observables list of observables which should be used before current observable
     *
     * @note `#include <rpp/operators/start_with.hpp>`
     *
     * @par Example
     * @snippet start_with.cpp start_with_observable
     * @snippet start_with.cpp start_with_observable_as_value
     *
     * @ingroup combining_operators
     * @see https://reactivex.io/documentation/operators/startwith.html
     */
    template<rpp::constraint::observable TObservable, rpp::constraint::observable... TObservables>
        requires constraint::observables_of_same_type<std::decay_t<TObservable>, std::decay_t<TObservables>...>
    auto start_with(TObservable&& observable, TObservables&&... observables)
    {
        return details::start_with_t<std::decay_t<TObservable>, std::decay_t<TObservables>...>{rpp::utils::tuple{std::forward<TObservable>(observable), std::forward<TObservables>(observables)...}};
    }

    /**
     * @brief Combines submissions from current observable with values into one but without overlapping and starting from values provided as arguments
     *
     * @marble start_with_values
         {
             source original_observable   : +-4--6-|
             operator "start_with_values(1,2,3)" : +-1-2-3--4--6-|
         }
     *
     * @details Actually it makes concat(rpp::source::just(vals_to_start_with)..., current_observable) so observables from argument subscribed before current observable
     * @details This overloading operates on rpp::schedulers::current_thread by default
     *
     * @tparam memory_model memory_model strategy used to store provided values
     * @param vals list of values which should be emitted before current observable
     *
     * @note `#include <rpp/operators/start_with.hpp>`
     *
     * @par Example
     * @snippet start_with.cpp start_with_values
     * @snippet start_with.cpp start_with_observable_as_value
     *
     * @ingroup combining_operators
     * @see https://reactivex.io/documentation/operators/startwith.html
     */
    template<constraint::memory_model MemoryModel /* = memory_model::use_stack */, typename T, typename... Ts>
        requires (rpp::constraint::decayed_same_as<T, Ts> && ...)
    auto start_with_values(T&& v, Ts&&... vals)
    {
        return start_with_values<MemoryModel>(rpp::schedulers::defaults::iteration_scheduler{}, std::forward<T>(v), std::forward<Ts>(vals)...);
    }

    /**
     * @brief Combines submissions from current observable with values into one but without overlapping and starting from values provided as arguments
     *
     * @marble start_with_values
         {
             source original_observable   : +-4--6-|
             operator "start_with_values(1,2,3)" : +-1-2-3--4--6-|
         }
     *
     * @details Actually it makes concat(rpp::source::just(vals_to_start_with)..., current_observable) so observables from argument subscribed before current observable
     *
     * @tparam memory_model memory_model strategy used to store provided values
     * @param vals list of values which should be emitted before current observable
     *
     * @note `#include <rpp/operators/start_with.hpp>`
     *
     * @par Example
     * @snippet start_with.cpp start_with_values
     *
     * @ingroup combining_operators
     * @see https://reactivex.io/documentation/operators/startwith.html
     */
    template<constraint::memory_model MemoryModel /* = memory_model::use_stack */, rpp::schedulers::constraint::scheduler TScheduler, typename T, typename... Ts>
        requires (rpp::constraint::decayed_same_as<T, Ts> && ...)
    auto start_with_values(const TScheduler& scheduler, T&& v, Ts&&... vals)
    {
        using inner_container = std::array<std::decay_t<T>, sizeof...(Ts) + 1>;
        using container       = std::conditional_t<std::same_as<MemoryModel, rpp::memory_model::use_stack>, inner_container, rpp::details::shared_container<inner_container>>;

        return details::start_with_values_t<container, TScheduler>{scheduler, std::forward<T>(v), std::forward<Ts>(vals)...};
    }

    template<constraint::memory_model MemoryModel /* = memory_model::use_stack*/, typename T, typename... Ts>
        requires ((rpp::constraint::decayed_same_as<T, Ts> && ...) && !(rpp::constraint::observable<T> || (rpp::constraint::observable<Ts> || ...)))
    auto start_with(T&& v, Ts&&... vals)
    {
        return start_with_values<MemoryModel>(std::forward<T>(v), std::forward<Ts>(vals)...);
    }

    template<constraint::memory_model MemoryModel /* = memory_model::use_stack*/, rpp::schedulers::constraint::scheduler TScheduler, typename T, typename... Ts>
        requires ((rpp::constraint::decayed_same_as<T, Ts> && ...) && !(rpp::constraint::observable<T> || (rpp::constraint::observable<Ts> || ...)))
    auto start_with(const TScheduler& scheduler, T&& v, Ts&&... vals)
    {
        return start_with_values<MemoryModel>(scheduler, std::forward<T>(v), std::forward<Ts>(vals)...);
    }
} // namespace rpp::operators
