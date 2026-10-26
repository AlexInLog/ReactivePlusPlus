//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/observers/fwd.hpp>

#include <rpp/utils/constraints.hpp>

namespace rpp::details::observables
{
    struct dynamic_disposables_strategy
    {
        template<size_t Count>
        using add = dynamic_disposables_strategy;

        using disposables_container         = disposables::dynamic_disposables_container;
        using observer_disposables_strategy = observers::dynamic_disposables_strategy;
    };

    template<size_t Count>
    struct fixed_disposables_strategy
    {
        template<size_t AddCount>
        using add = fixed_disposables_strategy<Count + AddCount>;

        using disposables_container         = disposables::static_disposables_container<Count>;
        using observer_disposables_strategy = observers::static_disposables_strategy<Count>;
    };

    using default_disposables_strategy = dynamic_disposables_strategy;

    namespace constraint
    {
        template<typename T>
        concept disposables_strategy = requires(const T&) {
            typename T::template add<size_t{}>;
            typename T::observer_disposables_strategy;
            typename T::disposables_container;
            requires observers::constraint::disposables_strategy<typename T::observer_disposables_strategy>;
        };
    } // namespace constraint
} // namespace rpp::details::observables
