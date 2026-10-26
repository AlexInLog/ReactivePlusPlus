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
#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/operators/details/strategy.hpp>
#include <rpp/schedulers/current_thread.hpp>
#include <rpp/utils/tuple.hpp>
#include <rpp/utils/utils.hpp>

#include <atomic>

namespace rpp::operators::details
{
    template<rpp::constraint::observer TObserver>
    class merge_disposable final : public composite_disposable
    {
    public:
        merge_disposable(TObserver&& observer)
            : m_observer(std::move(observer))
        {
        }

        // just need atomicity, not guarding anything
        void increment_on_completed() { m_on_completed_needed.fetch_add(1, std::memory_order::seq_cst); }

        // just need atomicity, not guarding anything
        bool decrement_on_completed() { return m_on_completed_needed.fetch_sub(1, std::memory_order::seq_cst) == 1; }

        rpp::utils::pointer_under_lock<TObserver> get_observer_under_lock() { return m_observer; }

    private:
        rpp::utils::value_with_mutex<TObserver> m_observer{};
        std::atomic_size_t                      m_on_completed_needed{1};
    };

    template<rpp::constraint::observer TObserver>
    struct merge_observer_base_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::Boolean;
        merge_observer_base_strategy(std::shared_ptr<merge_disposable<TObserver>>&& disposable)
            : m_disposable{std::move(disposable)}
        {
        }

        merge_observer_base_strategy(const std::shared_ptr<merge_disposable<TObserver>>& disposable)
            : m_disposable{disposable}
        {
        }

        void set_upstream(const rpp::disposable_wrapper& d) const
        {
            m_disposable->add(d);
            m_disposables.push_back(d);
        }

        bool is_disposed() const
        {
            return m_disposable->is_disposed();
        }

        void on_error(const std::exception_ptr& err) const
        {
            m_disposable->get_observer_under_lock()->on_error(err);
        }

        void on_completed() const
        {
            if (m_disposable->decrement_on_completed())
            {
                m_disposable->get_observer_under_lock()->on_completed();
            }
            else
            {
                for (const auto& v : m_disposables)
                {
                    m_disposable->remove(v);
                    v.dispose();
                }
            }
        }

    protected:
        std::shared_ptr<merge_disposable<TObserver>> m_disposable;
        mutable std::vector<rpp::disposable_wrapper> m_disposables{};
    };

    template<rpp::constraint::observer TObserver>
    struct merge_observer_inner_strategy final : public merge_observer_base_strategy<TObserver>
    {
        using merge_observer_base_strategy<TObserver>::merge_observer_base_strategy;

        template<typename T>
        void on_next(T&& v) const
        {
            merge_observer_base_strategy<TObserver>::m_disposable->get_observer_under_lock()->on_next(std::forward<T>(v));
        }
    };

    template<rpp::constraint::observer TObserver>
    class merge_observer_strategy final : public merge_observer_base_strategy<TObserver>
    {
    public:
        explicit merge_observer_strategy(TObserver&& observer)
            : merge_observer_base_strategy<TObserver>{init_state(std::move(observer))}
        {
        }

        template<typename T>
        void on_next(T&& v) const
        {
            merge_observer_base_strategy<TObserver>::m_disposable->increment_on_completed();
            std::forward<T>(v).subscribe(rpp::observer<rpp::utils::extract_observer_type_t<TObserver>, merge_observer_inner_strategy<TObserver>>{merge_observer_inner_strategy<TObserver>{merge_observer_base_strategy<TObserver>::m_disposable}});
        }

    private:
        static std::shared_ptr<merge_disposable<TObserver>> init_state(TObserver&& observer)
        {
            const auto d   = disposable_wrapper_impl<merge_disposable<TObserver>>::make(std::move(observer));
            auto       ptr = d.lock();
            ptr->get_observer_under_lock()->set_upstream(d.as_weak());
            return ptr;
        }
    };

    struct merge_t : lift_operator<merge_t>
    {
        using lift_operator<merge_t>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            static_assert(rpp::constraint::observable<T>, "T is not observable");

            using result_type = rpp::utils::extract_observable_type_t<T>;

            constexpr static bool own_current_queue = true;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = merge_observer_strategy<std::decay_t<TObserver>>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<1>;
    };

    template<rpp::constraint::observable... TObservables>
    struct merge_with_t
    {
        RPP_NO_UNIQUE_ADDRESS rpp::utils::tuple<TObservables...> observables{};

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            static_assert((std::same_as<T, rpp::utils::extract_observable_type_t<TObservables>> && ...), "T is not same as values of other observables");

            using result_type = T;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<1>;

        template<rpp::constraint::observer Observer, typename... Strategies>
        void subscribe(Observer&& observer, const rpp::details::observables::chain<Strategies...>& observable_strategy) const
        {
            merge_observer_strategy<std::decay_t<Observer>> strategy{std::forward<Observer>(observer)};

            // Need to take ownership over current_thread in case of inner-observables also using it
            auto drain_on_exit = rpp::schedulers::current_thread::own_queue_and_drain_finally_if_not_owned();

            strategy.on_next(observable_strategy);
            observables.apply(&apply<std::decay_t<Observer>>, strategy);
            strategy.on_completed();
        }

    private:
        template<rpp::constraint::observer Observer>
        static void apply(const merge_observer_strategy<Observer>& strategy, const TObservables&... observables)
        {
            (strategy.on_next(observables), ...);
        }
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Converts observable of observables of items into observable of items via merging emissions.
     *
     * @invariant According to observable contract (https://reactivex.io/documentation/contract.html) emissions from any observable should be serialized, so, resulting observable uses mutex to satisfy this requirement
     *
     * @attention During on subscribe operator takes ownership over rpp::schedulers::current_thread to allow mixing of underlying emissions
     *
     * @marble merge
         {
             source observable                :
             {
                 +--1-2-3-|
                 .....+4--6-|
             }
             operator "merge" : +--1-243-6-|
         }
     *
     * @details Actually it subscribes on each observable from emissions. Resulting observables completes when ALL observables completes
     *
     * @par Performance notes:
     * - 2 heap allocation (1 for state, 1 to convert observer to dynamic_observer)
     * - Acquiring mutex during all observer's calls
     *
     * @note `#include <rpp/operators/merge.hpp>`
     *
     * @par Example:
     * @snippet merge.cpp merge
     *
     * @ingroup combining_operators
     * @see https://reactivex.io/documentation/operators/merge.html
     */
    inline auto merge()
    {
        return details::merge_t{};
    }

    /**
     * @brief Combines submissions from current observable with other observables into one
     *
     * @warning According to observable contract (https://reactivex.io/documentation/contract.html) emissions from any observable should be serialized, so, resulting observable uses mutex to satisfy this requirement
     *
     * @warning During on subscribe operator takes ownership over rpp::schedulers::current_thread to allow mixing of underlying emissions
     *
     * @marble merge_with
         {
             source original_observable: +--1-2-3-|
             source second: +-----4--6-|
             operator "merge_with" : +--1-243-6-|
         }
     *
     * @details Actually it subscribes on each observable. Resulting observables completes when ALL observables completes
     *
     * @par Performance notes:
     * - 2 heap allocation (1 for state, 1 to convert observer to dynamic_observer)
     * - Acquiring mutex during all observer's calls
     *
     * @param observables are observables whose emissions would be merged with current observable
     * @note `#include <rpp/operators/merge.hpp>`
     *
     * @par Example:
     * @snippet merge.cpp merge_with
     *
     * @ingroup combining_operators
     * @see https://reactivex.io/documentation/operators/merge.html
     */
    template<rpp::constraint::observable TObservable, rpp::constraint::observable... TObservables>
        requires constraint::observables_of_same_type<std::decay_t<TObservable>, std::decay_t<TObservables>...>
    auto merge_with(TObservable&& observable, TObservables&&... observables)
    {
        return details::merge_with_t<std::decay_t<TObservable>, std::decay_t<TObservables>...>{
            rpp::utils::tuple{std::forward<TObservable>(observable), std::forward<TObservables>(observables)...}};
    }
} // namespace rpp::operators
