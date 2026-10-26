//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/sources/fwd.hpp>

#include <rpp/defs.hpp>
#include <rpp/observables/observable.hpp>
#include <rpp/operators/map.hpp>
#include <rpp/schedulers/current_thread.hpp>
#include <rpp/utils/utils.hpp>

#include <array>
#include <exception>
#include <memory>
#include <type_traits>
#include <utility>

namespace rpp::details
{
    template<constraint::decayed_type Container>
    class shared_container
    {
    public:
        template<typename... Ts>
            requires (!constraint::variadic_decayed_same_as<shared_container<Container>, Ts...>)
        explicit shared_container(Ts&&... items)
            // raw "new" call to avoid extra copy/moves for items
            : m_container{new Container{std::forward<Ts>(items)...}}
        {
        }

        shared_container(const shared_container&)     = default;
        shared_container(shared_container&&) noexcept = default;

        auto begin() const { return std::cbegin(*m_container); }

        auto end() const { return std::cend(*m_container); }

    private:
        std::shared_ptr<Container> m_container{};
    };

    struct from_iterable_schedulable
    {
        template<constraint::decayed_type PackedContainer, constraint::observer_strategy<utils::iterable_value_t<PackedContainer>> Strategy>
        rpp::schedulers::optional_delay_from_now operator()(const observer<utils::iterable_value_t<PackedContainer>, Strategy>& obs, const PackedContainer& cont, size_t& index) const
        {
            try
            {
                auto itr = std::cbegin(cont);
                auto end = std::cend(cont);
                std::advance(itr, static_cast<int64_t>(index));

                if (itr != end)
                {
                    obs.on_next(utils::as_const(*itr));
                    if (std::next(itr) != end) // it was not last
                    {
                        ++index;
                        return schedulers::delay_from_now{}; // re-schedule this
                    }
                }

                obs.on_completed();
            }
            catch (...)
            {
                obs.on_error(std::current_exception());
            }
            return std::nullopt;
        }
    };

    template<constraint::decayed_type PackedContainer, schedulers::constraint::scheduler TScheduler>
    struct from_iterable_strategy
    {
    public:
        using value_type                   = rpp::utils::iterable_value_t<PackedContainer>;
        using optimal_disposables_strategy = rpp::details::observables::fixed_disposables_strategy<0>;

        template<typename... Args>
        from_iterable_strategy(const TScheduler& scheduler, Args&&... args)
            : container{std::forward<Args>(args)...}
            , scheduler{scheduler}
        {
        }


        RPP_NO_UNIQUE_ADDRESS PackedContainer container;
        RPP_NO_UNIQUE_ADDRESS TScheduler      scheduler;

        template<constraint::observer_strategy<utils::iterable_value_t<PackedContainer>> Strategy>
        void subscribe(observer<utils::iterable_value_t<PackedContainer>, Strategy>&& obs) const
        {
            if constexpr (std::same_as<TScheduler, schedulers::immediate>)
            {
                try
                {
                    for (const auto& v : container)
                    {
                        if (obs.is_disposed())
                            return;

                        obs.on_next(v);
                    }

                    obs.on_completed();
                }
                catch (...)
                {
                    obs.on_error(std::current_exception());
                }
            }
            else
            {
                const auto worker = scheduler.create_worker();
                worker.schedule(from_iterable_schedulable{}, std::move(obs), container, size_t{});
            }
        }
    };

    template<typename PackedContainer, schedulers::constraint::scheduler TScheduler, typename... Args>
    auto make_from_iterable_observable(const TScheduler& scheduler, Args&&... args)
    {
        return observable<utils::iterable_value_t<std::decay_t<PackedContainer>>,
                          details::from_iterable_strategy<std::decay_t<PackedContainer>, TScheduler>>{scheduler, std::forward<Args>(args)...};
    }

    struct from_callable_invoke
    {
        template<typename Callable>
        auto operator()(Callable&& fn) const
        {
            if constexpr (std::same_as<utils::decayed_invoke_result_t<Callable>, void>)
            {
                fn();
                return rpp::utils::none{};
            }
            else
            {
                return fn();
            }
        }
    };
} // namespace rpp::details

namespace rpp::source
{
    /**
     * @brief Creates observable that emits a items from provided iterable
     *
     * @marble from_iterable
       {
           operator "from_iterable({1,2,3,5})": +-1-2-3-5-|
       }
     *
     * @tparam memory_model rpp::memory_model strategy used to handle provided iterable
     * @param scheduler is scheduler used for scheduling of submissions: next item will be submitted to scheduler when previous one is executed
     * @param iterable container with values which will be flattened
     *
     * @par Examples:
     * @snippet from.cpp from_iterable
     * @snippet from.cpp from_iterable with model
     * @snippet from.cpp from_iterable with scheduler
     *
     * @ingroup creational_operators
     * @see https://reactivex.io/documentation/operators/from.html
     */
    template<constraint::memory_model MemoryModel /* = memory_model::use_stack*/, constraint::iterable Iterable, schedulers::constraint::scheduler TScheduler /* = rpp::schedulers::defaults::iteration_scheduler*/>
    auto from_iterable(Iterable&& iterable, const TScheduler& scheduler /* = TScheduler{}*/)
    {
        using container = std::conditional_t<std::same_as<MemoryModel, rpp::memory_model::use_stack>, std::decay_t<Iterable>, details::shared_container<std::decay_t<Iterable>>>;
        return details::make_from_iterable_observable<container>(scheduler, std::forward<Iterable>(iterable));
    }

    /**
     * @brief Creates rpp::observable that emits a particular items and completes
     *
     * @marble just
       {
           operator "just(1,2,3,5)": +-1-2-3-5-|
       }
     *
     * @tparam memory_model rpp::memory_model startegy used to handle provided items
     * @tparam Scheduler type of scheduler used for scheduling of submissions: next item will be submitted to scheduler when previous one is executed
     * @param item first value to be sent
     * @param items rest values to be sent
     *
     * @par Examples:
     * @snippet just.cpp just
     * @snippet just.cpp just memory model
     * @snippet just.cpp just scheduler
     *
     * @ingroup creational_operators
     * @see https://reactivex.io/documentation/operators/just.html
     */
    template<constraint::memory_model MemoryModel /* = memory_model::use_stack */, schedulers::constraint::scheduler TScheduler, typename T, typename... Ts>
        requires (constraint::decayed_same_as<T, Ts> && ...)
    auto just(const TScheduler& scheduler, T&& item, Ts&&... items)
    {
        using inner_container = std::array<std::decay_t<T>, sizeof...(Ts) + 1>;
        using container       = std::conditional_t<std::same_as<MemoryModel, rpp::memory_model::use_stack>, inner_container, details::shared_container<inner_container>>;
        return details::make_from_iterable_observable<container>(scheduler, std::forward<T>(item), std::forward<Ts>(items)...);
    }

    /**
     * @brief Creates rpp::observable that emits a particular items and completes
     * @warning this overloading uses trampoline scheduler as default
     *
     * @marble just
       {
           operator "just(1,2,3,5)": +-1-2-3-5-|
       }
     *
     * @tparam memory_model rpp::memory_model strategy used to handle provided items
     * @param item first value to be sent
     * @param items rest values to be sent
     *
     * @par Examples:
     * @snippet just.cpp just
     * @snippet just.cpp just memory model
     * @snippet just.cpp just scheduler
     *
     * @ingroup creational_operators
     * @see https://reactivex.io/documentation/operators/just.html
     */
    template<constraint::memory_model MemoryModel /* = memory_model::use_stack */, typename T, typename... Ts>
        requires (constraint::decayed_same_as<T, Ts> && ...)
    auto just(T&& item, Ts&&... items)
    {
        using inner_container = std::array<std::decay_t<T>, sizeof...(Ts) + 1>;
        using container       = std::conditional_t<std::same_as<MemoryModel, rpp::memory_model::use_stack>, inner_container, details::shared_container<inner_container>>;
        return details::make_from_iterable_observable<container>(rpp::schedulers::defaults::iteration_scheduler{}, std::forward<T>(item), std::forward<Ts>(items)...);
    }

    /**
     * @brief Creates rpp::specific_observable that calls provided callable and emits resulting value of this callable
     *
     * @marble from_callable
       {
           operator "from_callable: [](){return 42;}": +-(42)--|
       }
     *
     * @tparam memory_model rpp::memory_model strategy used to handle callable
     *
     * @par Example
     * @snippet from.cpp from_callable
     *
     * @ingroup creational_operators
     * @see https://reactivex.io/documentation/operators/from.html
     */
    template<constraint::memory_model MemoryModel /* = memory_model::use_stack */, std::invocable<> Callable>
    auto from_callable(Callable&& callable)
    {
        return just<MemoryModel>(std::forward<Callable>(callable)) | rpp::operators::map(details::from_callable_invoke{});
    }
} // namespace rpp::source
