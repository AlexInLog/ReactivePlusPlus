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
#include <rpp/schedulers/current_thread.hpp>
#include <rpp/utils/utils.hpp>

#include <list>

namespace rpp
{
    template<constraint::decayed_type Type>
    using window_toggle_observable = decltype(std::declval<rpp::operators::details::forwarding_subject<Type>>().get_observable());
} // namespace rpp

namespace rpp::operators::details
{
    template<rpp::constraint::observer TObserver, typename TClosingsSelectorFn>
    struct window_toggle_state
    {
        using Observable = rpp::utils::extract_observer_type_t<TObserver>;
        using value_type = rpp::utils::extract_observable_type_t<Observable>;
        using Subject    = forwarding_subject<value_type>;

        static_assert(std::same_as<Observable, decltype(std::declval<Subject>().get_observable())>);

        struct state_t
        {
            RPP_NO_UNIQUE_ADDRESS TObserver                             observer;
            std::list<decltype(std::declval<Subject>().get_observer())> observers{};
        };

        window_toggle_state(TObserver&& observer, const TClosingsSelectorFn& closings)
            : m_state{state_t{std::move(observer)}}
            , m_closings{closings}
        {
        }

        rpp::utils::pointer_under_lock<state_t> get_state_under_lock() { return rpp::utils::pointer_under_lock<state_t>{m_state}; }

        template<typename T>
        auto get_closing(T&& v) const
        {
            return m_closings(std::forward<T>(v));
        }

        auto on_new_subject(const Subject& subject)
        {
            auto       locked_state = get_state_under_lock();
            const auto itr          = locked_state->observers.insert(locked_state->observers.cend(), subject.get_observer());
            locked_state->observer.on_next(subject.get_observable());
            return itr;
        }

    private:
        rpp::utils::value_with_mutex<state_t>     m_state{};
        RPP_NO_UNIQUE_ADDRESS TClosingsSelectorFn m_closings;
    };

    template<rpp::constraint::decayed_type TState>
    struct window_toggle_closing_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        std::shared_ptr<rpp::refcount_disposable>                                                 disposable;
        std::shared_ptr<TState>                                                                   state;
        rpp::composite_disposable_wrapper                                                         this_disposable;
        decltype(std::declval<TState>().on_new_subject(std::declval<typename TState::Subject>())) itr;

        void on_next(const auto&) const
        {
            on_completed();
        }

        void on_error(const std::exception_ptr& err) const
        {
            auto locked_state = state->get_state_under_lock();
            for (const auto& obs : locked_state->observers)
                obs.on_error(err);
            locked_state->observer.on_error(err);
        }

        void on_completed() const
        {
            disposable->remove(this_disposable);

            itr->on_completed();
            this_disposable.dispose();

            auto locked_state = state->get_state_under_lock();
            locked_state->observers.erase(itr);
        }

        void set_upstream(const disposable_wrapper& d) const { this_disposable.add(d); }
        bool is_disposed() const { return this_disposable.is_disposed(); }
    };

    template<rpp::constraint::decayed_type TState>
    struct window_toggle_opening_observer_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::Auto;

        std::shared_ptr<rpp::refcount_disposable> disposable;
        std::shared_ptr<TState>                   state;

        template<typename T>
        void on_next(T&& v) const
        {
            typename TState::Subject subject{disposable->wrapper_from_this()};
            const auto               itr = state->on_new_subject(subject);

            disposable->add(subject.get_disposable());
            state->get_closing(std::forward<T>(v)).subscribe(window_toggle_closing_observer_strategy<TState>{disposable, state, subject.get_disposable(), itr});
        }

        void on_error(const std::exception_ptr& err) const
        {
            const auto locked_state = state->get_state_under_lock();
            for (const auto& obs : locked_state->observers)
                obs.on_error(err);
            locked_state->observer.on_error(err);
        }

        static void on_completed() { }
        static void set_upstream(const disposable_wrapper&) { }
        static bool is_disposed() { return false; }
    };

    template<rpp::constraint::observer TObserver, rpp::constraint::observable TOpeningsObservable, typename TClosingsSelectorFn>
        requires rpp::constraint::observable<std::invoke_result_t<TClosingsSelectorFn, rpp::utils::extract_observable_type_t<TOpeningsObservable>>>
    class window_toggle_observer_strategy
    {
        using TState = window_toggle_state<TObserver, TClosingsSelectorFn>;

    public:
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        window_toggle_observer_strategy(TObserver&& observer, const TOpeningsObservable& openings, const TClosingsSelectorFn& closings)
            : m_state{std::make_shared<TState>(std::move(observer), closings)}
        {
            m_state->get_state_under_lock()->observer.set_upstream(m_disposable->add_ref());
            m_disposable->add(openings.subscribe_with_disposable(window_toggle_opening_observer_strategy<TState>{m_disposable, m_state}));
        }

        void on_next(const auto& v) const
        {
            const auto locked_state = m_state->get_state_under_lock();
            for (const auto& obs : locked_state->observers)
                obs.on_next(v);
        }

        void on_error(const std::exception_ptr& err) const
        {
            const auto locked_state = m_state->get_state_under_lock();
            for (const auto& obs : locked_state->observers)
                obs.on_error(err);
            locked_state->observer.on_error(err);
        }

        void on_completed() const
        {
            const auto locked_state = m_state->get_state_under_lock();
            for (const auto& obs : locked_state->observers)
                obs.on_completed();
            locked_state->observer.on_completed();
        }

        void set_upstream(const disposable_wrapper& d) const { m_disposable->add(d); }

        bool is_disposed() const { return m_disposable->is_disposed(); }

    private:
        std::shared_ptr<rpp::refcount_disposable> m_disposable = disposable_wrapper_impl<rpp::refcount_disposable>::make().lock();
        std::shared_ptr<TState>                   m_state;
    };

    template<rpp::constraint::observable TOpeningsObservable, typename TClosingsSelectorFn>
        requires rpp::constraint::observable<std::invoke_result_t<TClosingsSelectorFn, rpp::utils::extract_observable_type_t<TOpeningsObservable>>>
    struct window_toggle_t : lift_operator<window_toggle_t<TOpeningsObservable, TClosingsSelectorFn>, TOpeningsObservable, TClosingsSelectorFn>
    {
        using lift_operator<window_toggle_t<TOpeningsObservable, TClosingsSelectorFn>, TOpeningsObservable, TClosingsSelectorFn>::lift_operator;

        template<rpp::constraint::decayed_type T>
        struct operator_traits
        {
            using result_type = rpp::window_toggle_observable<T>;

            constexpr static bool own_current_queue = true;

            template<rpp::constraint::observer_of_type<result_type> TObserver>
            using observer_strategy = window_toggle_observer_strategy<std::decay_t<TObserver>, TOpeningsObservable, TClosingsSelectorFn>;
        };

        template<rpp::details::observables::constraint::disposables_strategy Prev>
        using updated_optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<1>;
    };
} // namespace rpp::operators::details

namespace rpp::operators
{
    /**
     * @brief Subdivide original observable into sub-observables (window observables) and emit sub-observables of items instead of original items
     * @details Values from `openings` observable used to specify moment when new window will be opened. `closings_selector` is used to obtain observable to specify moment when new window will be closed.
     *
     * @marble window_toggle
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
     * @param openings is observable which emissions used to start new window
     * @param closings_selector is function which returns observable which emission/completion means closing of opened window
     *
     * @note `#include <rpp/operators/window.hpp>`
     *
     * @par Example
     * @snippet window_toggle.cpp window_toggle
     *
     * @ingroup transforming_operators
     * @see https://reactivex.io/documentation/operators/window.html
     */
    template<rpp::constraint::observable TOpeningsObservable, typename TClosingsSelectorFn>
        requires rpp::constraint::observable<std::invoke_result_t<TClosingsSelectorFn, rpp::utils::extract_observable_type_t<TOpeningsObservable>>>
    auto window_toggle(TOpeningsObservable&& openings, TClosingsSelectorFn&& closings_selector)
    {
        return details::window_toggle_t<std::decay_t<TOpeningsObservable>, std::decay_t<TClosingsSelectorFn>>{std::forward<TOpeningsObservable>(openings), std::forward<TClosingsSelectorFn>(closings_selector)};
    }
} // namespace rpp::operators
