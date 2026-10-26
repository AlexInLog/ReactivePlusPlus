//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/defs.hpp>
#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/observers/observer.hpp>
#include <rpp/utils/function_traits.hpp>

namespace rpp::details::observers
{
    template<rpp::constraint::decayed_type             Type,
             std::invocable<Type>                      OnNext,
             std::invocable<const std::exception_ptr&> OnError,
             std::invocable<>                          OnCompleted>
    struct lambda_strategy
    {
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::Auto;

        template<rpp::constraint::decayed_same_as<OnNext> TOnNext, rpp::constraint::decayed_same_as<OnError> TOnError, rpp::constraint::decayed_same_as<OnCompleted> TOnCompleted>
        explicit lambda_strategy(TOnNext&& on_next, TOnError&& on_error, TOnCompleted&& on_completed)
            : on_next{std::forward<TOnNext>(on_next)}
            , on_error{std::forward<TOnError>(on_error)}
            , on_completed{std::forward<TOnCompleted>(on_completed)}
        {
        }

        RPP_NO_UNIQUE_ADDRESS OnNext      on_next{};
        RPP_NO_UNIQUE_ADDRESS OnError     on_error{};
        RPP_NO_UNIQUE_ADDRESS OnCompleted on_completed{};

        static void set_upstream(const disposable_wrapper&) noexcept { }

        static bool is_disposed() noexcept { return false; }
    };
} // namespace rpp::details::observers

namespace rpp
{
    template<constraint::decayed_type                  Type,
             std::invocable<Type>                      OnNext,
             std::invocable<const std::exception_ptr&> OnError,
             std::invocable<>                          OnCompleted>
    auto make_lambda_observer(OnNext&&      on_next,
                              OnError&&     on_error,
                              OnCompleted&& on_completed) -> lambda_observer<Type,
                                                                             std::decay_t<OnNext>,
                                                                             std::decay_t<OnError>,
                                                                             std::decay_t<OnCompleted>>
    {
        return lambda_observer<Type,
                               std::decay_t<OnNext>,
                               std::decay_t<OnError>,
                               std::decay_t<OnCompleted>>{
            std::forward<OnNext>(on_next),
            std::forward<OnError>(on_error),
            std::forward<OnCompleted>(on_completed)};
    }

    template<constraint::decayed_type                  Type,
             std::invocable<Type>                      OnNext,
             std::invocable<const std::exception_ptr&> OnError,
             std::invocable<>                          OnCompleted>
    auto make_lambda_observer(const rpp::composite_disposable_wrapper& d,
                              OnNext&&                                 on_next,
                              OnError&&                                on_error,
                              OnCompleted&&                            on_completed) -> lambda_observer_with_external_disposable<Type,
                                                                                                      std::decay_t<OnNext>,
                                                                                                      std::decay_t<OnError>,
                                                                                                      std::decay_t<OnCompleted>>
    {
        return lambda_observer_with_external_disposable<Type,
                                                        std::decay_t<OnNext>,
                                                        std::decay_t<OnError>,
                                                        std::decay_t<OnCompleted>>{
            d,
            std::forward<OnNext>(on_next),
            std::forward<OnError>(on_error),
            std::forward<OnCompleted>(on_completed)};
    }
} // namespace rpp
