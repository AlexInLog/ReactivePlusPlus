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
#include <rpp/operators/details/combining_strategy.hpp>
#include <rpp/operators/details/strategy.hpp>

namespace rpp::operators::details
{
    template<rpp::constraint::observer Observer, typename TSelector, rpp::constraint::decayed_type... Args>
    class combine_latest_disposable final : public combining_disposable<Observer>
    {
    public:
        explicit combine_latest_disposable(Observer&& observer, const TSelector& selector)
            : combining_disposable<Observer>(std::move(observer), sizeof...(Args))
            , m_selector(selector)
        {
        }

        const auto& get_selector() const { return m_selector; }

        auto& get_values() { return m_values; }

    private:
        rpp::utils::tuple<std::optional<Args>...> m_values{};

        RPP_NO_UNIQUE_ADDRESS TSelector m_selector;
    };

    template<size_t I, rpp::constraint::observer Observer, typename TSelector, rpp::constraint::decayed_type... Args>
    struct combine_latest_observer_strategy final
        : public combining_observer_strategy<combine_latest_disposable<Observer, TSelector, Args...>>
    {
        using combining_observer_strategy<combine_latest_disposable<Observer, TSelector, Args...>>::disposable;

        template<typename T>
        void on_next(T&& v) const
        {
            // mutex need to be locked during changing of values, generating new values and sending of new values due to we can't update value while we are sending old one
            const auto observer = disposable->get_observer_under_lock();
            disposable->get_values().template get<I>().emplace(std::forward<T>(v));

            disposable->get_values().apply(&apply_impl<decltype(disposable)>, disposable, observer);
        }

    private:
        template<typename TDisposable>
        static void apply_impl(const TDisposable& disposable, const rpp::utils::pointer_under_lock<Observer>& observer, const std::optional<Args>&... vals)
        {
            if ((vals.has_value() && ...))
                observer->on_next(disposable->get_selector()(vals.value()...));
        }
    };

    template<typename TSelector, rpp::constraint::observable... TObservables>
    struct combine_latest_t : public combining_operator_t<combine_latest_disposable, combine_latest_observer_strategy, TSelector, TObservables...>
    {
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Combines latest emissions from observables with emission from current observable when any observable sends new value via applying selector
     *
     * @marble combine_latest_custom_selector
       {
           source observable                                 : +------1    -2    --    -3    -|
           source other_observable                           : +-5-6-7-    --    -8    --    -|
           operator "combine_latest: x,y =>std::pair{x,y}"   : +------{1,5}-{2,7}-{2,8}-{3,8}-|
       }
     *
     * @details Actually this operator subscribes on all of theses observables and emits new combined value when any of them emits new emission (and each observable emit values at least one to be able to provide combined value)
     *
     * @par Performance notes:
     * - 1 heap allocation for disposable
     * - each value from any observable copied/moved to internal storage
     * - mutex acquired every time value obtained
     *
     * @param selector is applied to current emission of current observable and latests emissions from observables
     * @param observables are observables whose emissions would be combined with current observable
     * @note `#include <rpp/operators/combine_latest.hpp>`
     *
     * @par Examples
     * @snippet combine_latest.cpp combine_latest custom combiner
     *
     * @ingroup combining_operators
     * @see https://reactivex.io/documentation/operators/combinelatest.html
     */
    template<typename TSelector, rpp::constraint::observable TObservable, rpp::constraint::observable... TObservables>
        requires (!rpp::constraint::observable<TSelector> && (!utils::is_not_template_callable<TSelector> || std::invocable<TSelector, rpp::utils::convertible_to_any, utils::extract_observable_type_t<TObservable>, utils::extract_observable_type_t<TObservables>...>))
    auto combine_latest(TSelector&& selector, TObservable&& observable, TObservables&&... observables)
    {
        return details::combine_latest_t<std::decay_t<TSelector>, std::decay_t<TObservable>, std::decay_t<TObservables>...>{
            rpp::utils::tuple{std::forward<TObservable>(observable), std::forward<TObservables>(observables)...},
            std::forward<TSelector>(selector)};
    }

    /**
     * @brief Combines latest emissions from observables with emission from current observable when any observable sends new value via making tuple
     *
     * @marble combine_latest
       {
           source observable                       : +------1    -2    --    -3    -|
           source other_observable                 : +-5-6-7-    --    -8    --    -|
           operator "combine_latest: make_tuple"   : +------{1,5}-{2,7}-{2,8}-{3,8}-|
       }
     *
     * @details Actually this operator subscribes on all of theses observables and emits new combined value when any of them emits new emission (and each observable emit values at least one to be able to provide combined value)
     *
     * @warning Selector is just packing values to tuple in this case
     *
     * @par Performance notes:
     * - 1 heap allocation for disposable
     * - each value from any observable copied/moved to internal storage
     * - mutex acquired every time value obtained
     *
     * @param observables are observables whose emissions would be combined when any observable sends new value
     * @note `#include <rpp/operators/combine_latest.hpp>`
     *
     * @par Examples
     * @snippet combine_latest.cpp combine_latest
     *
     * @ingroup combining_operators
     * @see https://reactivex.io/documentation/operators/combinelatest.html
     */
    template<rpp::constraint::observable TObservable, rpp::constraint::observable... TObservables>
    auto combine_latest(TObservable&& observable, TObservables&&... observables)
    {
        return combine_latest(rpp::utils::pack_to_tuple{}, std::forward<TObservable>(observable), std::forward<TObservables>(observables)...);
    }
} // namespace rpp::operators
