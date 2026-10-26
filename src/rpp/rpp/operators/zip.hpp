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

#include <deque>

namespace rpp::operators::details
{
    template<rpp::constraint::observer Observer, typename TSelector, rpp::constraint::decayed_type... Args>
    class zip_disposable final : public combining_disposable<Observer>
    {
    public:
        explicit zip_disposable(Observer&& observer, const TSelector& selector)
            : combining_disposable<Observer>(std::move(observer), sizeof...(Args))
            , m_selector(selector)
        {
        }

        const auto& get_selector() const { return m_selector; }

        auto& get_pendings() { return m_pendings; }

    private:
        utils::tuple<std::deque<Args>...> m_pendings{};

        RPP_NO_UNIQUE_ADDRESS TSelector m_selector;
    };

    template<size_t I, rpp::constraint::observer Observer, typename TSelector, rpp::constraint::decayed_type... Args>
    struct zip_observer_strategy final
        : public combining_observer_strategy<zip_disposable<Observer, TSelector, Args...>>
    {
        using combining_observer_strategy<zip_disposable<Observer, TSelector, Args...>>::disposable;

        template<typename T>
        void on_next(T&& v) const
        {
            const auto observer = disposable->get_observer_under_lock();
            disposable->get_pendings().template get<I>().push_back(std::forward<T>(v));

            disposable->get_pendings().apply(&apply_impl<decltype(disposable)>, disposable, observer);
        }

    private:
        template<typename TDisposable>
        static void apply_impl(const TDisposable& disposable, const rpp::utils::pointer_under_lock<Observer>& observer, std::deque<Args>&... values)
        {
            if ((!values.empty() && ...))
            {
                observer->on_next(disposable->get_selector()(std::move(values.front())...));
                (values.pop_front(), ...);
            }
        }
    };

    template<typename TSelector, rpp::constraint::observable... TObservables>
    struct zip_t : public combining_operator_t<zip_disposable, zip_observer_strategy, TSelector, TObservables...>
    {
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief combines emissions from observables and emit single items for each combination based on the results of provided selector
     *
     * @marble zip_custom_selector
       {
           source observable                      : +------1    -2    -3--    ------|
           source other_observable                : +-5-6--     -     ---7    --8---|
           operator "zip: x,y =>std::pair{x,y}"   : +------{1,5}-{2,6}---{3,7}------|
       }
     *
     *
     * @par Performance notes:
     * - 1 heap allocation for disposable
     * - each value from any observable copied/moved to internal storage
     * - mutex acquired every time value obtained
     *
     * @param selector is applied to current emission of current observable and latests emissions from observables
     * @param observables are observables whose emissions would be zipped with current observable
     * @note `#include <rpp/operators/zip.hpp>`
     *
     * @ingroup combining_operators
     * @see https://reactivex.io/documentation/operators/zip.html
     */
    template<typename TSelector, rpp::constraint::observable TObservable, rpp::constraint::observable... TObservables>
        requires (!rpp::constraint::observable<TSelector> && (!utils::is_not_template_callable<TSelector> || std::invocable<TSelector, rpp::utils::convertible_to_any, utils::extract_observable_type_t<TObservable>, utils::extract_observable_type_t<TObservables>...>))
    auto zip(TSelector&& selector, TObservable&& observable, TObservables&&... observables)
    {
        return details::zip_t<std::decay_t<TSelector>, std::decay_t<TObservable>, std::decay_t<TObservables>...>{
            rpp::utils::tuple{std::forward<TObservable>(observable), std::forward<TObservables>(observables)...},
            std::forward<TSelector>(selector)};
    }

    /**
     * @brief combines emissions from observables and emit tuple of items for each combination
     *
     * @marble zip
       {
           source observable                      : +------1    -2    -3--    ------|
           source other_observable                : +-5-6--     -     ---7    --8---|
           operator "zip: make_tuple"             : +------{1,5}-{2,6}---{3,7}------|
       }
     *
     *
     * @par Performance notes:
     * - 1 heap allocation for disposable
     * - each value from any observable copied/moved to internal storage
     * - mutex acquired every time value obtained
     *
     * @param observables are observables whose emissions would be zipped with current observable
     * @note `#include <rpp/operators/zip.hpp>`
     *
     * @ingroup combining_operators
     * @see https://reactivex.io/documentation/operators/zip.html
     */
    template<rpp::constraint::observable TObservable, rpp::constraint::observable... TObservables>
    auto zip(TObservable&& observable, TObservables&&... observables)
    {
        return zip(rpp::utils::pack_to_tuple{}, std::forward<TObservable>(observable), std::forward<TObservables>(observables)...);
    }
} // namespace rpp::operators
