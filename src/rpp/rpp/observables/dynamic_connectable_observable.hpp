//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/observables/connectable_observable.hpp>
#include <rpp/observables/dynamic_observable.hpp>

namespace rpp
{
    template<typename Subject>
    class dynamic_connectable_observable final : public connectable_observable<rpp::dynamic_observable<rpp::subjects::utils::extract_subject_type_t<Subject>>, Subject>
    {
    public:
        static_assert(rpp::constraint::subject<Subject>);

        using base = connectable_observable<rpp::dynamic_observable<rpp::subjects::utils::extract_subject_type_t<Subject>>, Subject>;

        using base::base;

        template<rpp::constraint::observable_strategy<rpp::subjects::utils::extract_subject_type_t<Subject>> Strategy>
            requires (!rpp::constraint::decayed_same_as<Strategy, rpp::dynamic_observable<rpp::subjects::utils::extract_subject_type_t<Subject>>>)
        dynamic_connectable_observable(const rpp::connectable_observable<Strategy, Subject>& original)
            : dynamic_connectable_observable{original.as_dynamic_connectable()}
        {
        }

        template<rpp::constraint::observable_strategy<rpp::subjects::utils::extract_subject_type_t<Subject>> Strategy>
            requires (!rpp::constraint::decayed_same_as<Strategy, rpp::dynamic_observable<rpp::subjects::utils::extract_subject_type_t<Subject>>>)
        dynamic_connectable_observable(rpp::connectable_observable<Strategy, Subject>&& original)
            : dynamic_connectable_observable{std::move(original).as_dynamic_connectable()}
        {
        }
    };
} // namespace rpp
