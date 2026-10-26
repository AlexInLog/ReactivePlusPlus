//                  ReactivePlusPlus library
//
//          Copyright Aleksey Loginov 2022 - present.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          https://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/AlexInLog/ReactivePlusPlus
//

#pragma once

#include <rpp/subjects/publish_subject.hpp>

#include <rppqt/sources/fwd.hpp>

#include <QObject>
#include <tuple>

namespace rppqt::details
{
    template<typename... Args>
    struct from_signal_on_event
    {
        using subject = rpp::subjects::publish_subject<std::tuple<std::decay_t<Args>...>>;

        decltype(std::declval<subject>().get_observer()) observer;

        template<typename... Vals>
        void operator()(Vals&&... vals) const
        {
            observer.on_next(std::make_tuple(std::forward<Vals>(vals)...));
        }
    };

    template<typename Arg>
    struct from_signal_on_event<Arg>
    {
        using subject = rpp::subjects::publish_subject<std::decay_t<Arg>>;
        decltype(std::declval<subject>().get_observer()) observer;

        template<rpp::constraint::decayed_same_as<Arg> Val>
        void operator()(Val&& val) const
        {
            observer.on_next(std::forward<Val>(val));
        }
    };

    template<>
    struct from_signal_on_event<>
    {
        using subject = rpp::subjects::publish_subject<rpp::utils::none>;
        decltype(std::declval<subject>().get_observer()) observer;

        void operator()() const
        {
            observer.on_next(rpp::utils::none{});
        }
    };
} // namespace rppqt::details

namespace rppqt::source
{
    /**
     * @brief Creates rpp::observable that emits a items from provided QT signal
     *
     * @param object is QObject which would emit signals
     * @param signal is interested signal which would generate emissions for observable. Expected to obtain pointer to member function representing signal
     *
     * @par Examples:
     * @snippet from_signal.cpp from_signal
     *
     * @ingroup qt_creational_operators
     */
    template<std::derived_from<QObject> TSignalQObject, std::derived_from<TSignalQObject> TObject, typename R, typename... Args>
    auto from_signal(const TObject& object, R (TSignalQObject::*signal)(Args...))
    {
        using on_next_impl = details::from_signal_on_event<Args...>;
        const auto subj    = typename on_next_impl::subject{};

        QObject::connect(&object, signal, on_next_impl{subj.get_observer()});
        QObject::connect(&object, &QObject::destroyed, [observer = subj.get_observer()] { observer.on_completed(); });

        return subj.get_observable();
    }
} // namespace rppqt::source
