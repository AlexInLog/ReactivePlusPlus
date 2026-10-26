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

#include <rpp/disposables/refcount_disposable.hpp>
#include <rpp/observables/grouped_observable.hpp>
#include <rpp/operators/details/strategy.hpp>
#include <rpp/subjects/publish_subject.hpp>
#include <rpp/utils/function_traits.hpp>

#include <map>
#include <type_traits>

namespace rpp::operators::details
{
    template<rpp::constraint::decayed_type T>
    struct group_by_observable_strategy;
} // namespace rpp::operators::details

namespace rpp
{
    template<constraint::decayed_type TKey, constraint::decayed_type ResValue>
    using grouped_observable_group_by = grouped_observable<TKey, ResValue, operators::details::group_by_observable_strategy<ResValue>>;
} // namespace rpp

namespace rpp::operators::details
{
    template<rpp::constraint::observer TObserver>
    struct group_by_inner_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        RPP_NO_UNIQUE_ADDRESS TObserver   observer;
        rpp::composite_disposable_wrapper disposable;

        template<typename T>
        void on_next(T&& v) const
        {
            observer.on_next(std::forward<T>(v));
        }

        void on_error(const std::exception_ptr& err) const { observer.on_error(err); }

        void on_completed() const { observer.on_completed(); }

        bool is_disposed() const { return observer.is_disposed(); }

        void set_upstream(const disposable_wrapper& d) const { disposable.add(d); }
    };

    template<rpp::constraint::decayed_type T, rpp::constraint::observer TObserver, rpp::constraint::decayed_type KeySelector, rpp::constraint::decayed_type ValueSelector, rpp::constraint::decayed_type KeyComparator>
    struct group_by_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        using TKey = rpp::utils::decayed_invoke_result_t<KeySelector, T>;
        using Type = rpp::utils::decayed_invoke_result_t<ValueSelector, T>;

        RPP_NO_UNIQUE_ADDRESS TObserver     observer;
        RPP_NO_UNIQUE_ADDRESS KeySelector   key_selector;
        RPP_NO_UNIQUE_ADDRESS ValueSelector value_selector;
        RPP_NO_UNIQUE_ADDRESS KeyComparator comparator;

        using subject_observer = decltype(std::declval<subjects::publish_subject<Type>>().get_observer());

        mutable std::map<TKey, subject_observer, KeyComparator> key_to_observer{};
        std::shared_ptr<refcount_disposable>                    disposable = [&] {
            auto ptr = disposable_wrapper_impl<refcount_disposable>::make().lock();
            observer.set_upstream(ptr->add_ref());
            return ptr;
        }();

        void set_upstream(const rpp::disposable_wrapper& d) const
        {
            disposable->add(d);
        }

        bool is_disposed() const
        {
            return disposable->is_disposed();
        }

        template<rpp::constraint::decayed_same_as<T> TT>
        void on_next(TT&& val) const
        {
            const auto subject_observer = deduce_observer(observer, val);
            if (subject_observer && !subject_observer->is_disposed())
                subject_observer->on_next(value_selector(std::forward<TT>(val)));
        }

        void on_error(const std::exception_ptr& err) const
        {
            for (const auto& [key, subject_observer] : key_to_observer)
                subject_observer.on_error(err);

            observer.on_error(err);
        }

        void on_completed() const
        {
            for (const auto& [key, subject_observer] : key_to_observer)
                subject_observer.on_completed();

            observer.on_completed();
        }

    private:
        template<rpp::constraint::decayed_same_as<T> TT>
        const subject_observer* deduce_observer(const rpp::constraint::observer auto& obs, const TT& val) const
        {
            const auto key = key_selector(utils::as_const(val));

            if (const auto itr = key_to_observer.find(key); itr != key_to_observer.cend())
                return &itr->second;

            if (obs.is_disposed())
                return nullptr;

            const subjects::publish_subject<Type> subj{};

            disposable->add(subj.get_disposable().as_weak());
            obs.on_next(rpp::grouped_observable_group_by<TKey, Type>{
                key,
                group_by_observable_strategy<Type>{subj, disposable}});

            return &key_to_observer.emplace(key, subj.get_observer()).first->second;
        }
    };

    template<rpp::constraint::decayed_type T>
    struct group_by_observable_strategy
    {
        using value_type                   = T;
        using optimal_disposables_strategy = typename rpp::subjects::publish_subject<T>::optimal_disposables_strategy;

        rpp::subjects::publish_subject<T>  subj;
        std::weak_ptr<refcount_disposable> disposable;

        template<rpp::constraint::observer_strategy<T> Strategy>
        void subscribe(observer<T, Strategy>&& obs) const
        {
            if (const auto locked = disposable.lock())
            {
                auto d = locked->add_ref();
                obs.set_upstream(d);
                subj.get_observable()
                    .subscribe(rpp::observer<T, group_by_inner_observer_strategy<observer<T, Strategy>>>{std::move(obs), std::move(d)});
            }
        }
    };

    template<rpp::constraint::decayed_type KeySelector, rpp::constraint::decayed_type ValueSelector, rpp::constraint::decayed_type KeyComparator>
    struct group_by_t : lift_operator<group_by_t<KeySelector, ValueSelector, KeyComparator>, KeySelector, ValueSelector, KeyComparator>
    {
        using operators::details::lift_operator<group_by_t<KeySelector, ValueSelector, KeyComparator>, KeySelector, ValueSelector, KeyComparator>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            static_assert(!std::same_as<void, std::invoke_result_t<KeySelector, T>>, "KeySelector is not invocacble with T");
            static_assert(!std::same_as<void, std::invoke_result_t<ValueSelector, T>>, "ValueSelector is not invocable with T");
            static_assert(std::strict_weak_order<KeyComparator, rpp::utils::decayed_invoke_result_t<KeySelector, T>, rpp::utils::decayed_invoke_result_t<KeySelector, T>>, "KeyComparator is not invocable with result of KeySelector");

            using result_type = grouped_observable<utils::decayed_invoke_result_t<KeySelector, T>, rpp::utils::decayed_invoke_result_t<ValueSelector, T>, group_by_observable_strategy<utils::decayed_invoke_result_t<ValueSelector, T>>>;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = group_by_observer_strategy<T, TObserver, KeySelector, ValueSelector, KeyComparator>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<1>;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Divide original observable into multiple observables where each new observable emits some group of values from original observable.
     *
     * @marble group_by
        {
             source observable              : +--1-2-3-4-5-6-|
             operator "group_by(x=>x%2==0)" :
             {
                                              ..+1---3---5---|
                                              ....+2---4---6-|
             }
        }
     *
     *
     * @details Actually this operator applies `key_selector` to emission to obtain key, place rpp::grouped_observable to map with corresponding map and then send observable with this key (if not yet). Original values emitted via this grouped_observables
     *
     * @param key_selector Function which determines key for provided item
     * @param value_selector Function which determines value to be emitted to grouped observable
     * @param comparator Function to provide strict_weak_order between key types
     *
     * @note `#include <rpp/operators/group_by.hpp>`
     *
     * @par Example:
     * @snippet group_by.cpp group_by
     * @snippet group_by.cpp group_by selector
     *
     * @ingroup transforming_operators
     * @see https://reactivex.io/documentation/operators/groupby.html
     */
    template<typename KeySelector,
             typename ValueSelector,
             typename KeyComparator>
        requires (
            (!utils::is_not_template_callable<KeySelector> || !std::same_as<void, std::invoke_result_t<KeySelector, rpp::utils::convertible_to_any>>) && (!utils::is_not_template_callable<ValueSelector> || !std::same_as<void, std::invoke_result_t<ValueSelector, rpp::utils::convertible_to_any>>) && (!utils::is_not_template_callable<KeyComparator> || std::strict_weak_order<KeyComparator, rpp::utils::convertible_to_any, rpp::utils::convertible_to_any>))
    auto group_by(KeySelector&& key_selector, ValueSelector&& value_selector, KeyComparator&& comparator)
    {
        return details::group_by_t<std::decay_t<KeySelector>, std::decay_t<ValueSelector>, std::decay_t<KeyComparator>>{
            std::forward<KeySelector>(key_selector),
            std::forward<ValueSelector>(value_selector),
            std::forward<KeyComparator>(comparator)};
    }
} // namespace rpp::operators
