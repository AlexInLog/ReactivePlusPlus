//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2022 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/disposables/fwd.hpp>
#include <rpp/observables/fwd.hpp>
#include <rpp/observers/fwd.hpp>

#include <rpp/utils/constraints.hpp>
#include <rpp/utils/utils.hpp>

namespace rpp::subjects
{
    template<rpp::constraint::decayed_type Type>
    class publish_subject;

    template<rpp::constraint::decayed_type Type>
    class serialized_publish_subject;


    template<rpp::constraint::decayed_type Type>
    class replay_subject;

    template<rpp::constraint::decayed_type Type>
    class serialized_replay_subject;


    template<rpp::constraint::decayed_type Type>
    class behavior_subject;

    template<rpp::constraint::decayed_type Type>
    class serialized_behavior_subject;


} // namespace rpp::subjects

namespace rpp::constraint
{
    template<typename T>
    concept subject = requires(const T& subj) {
        {
            subj.get_observer()
        } -> rpp::constraint::observer;
        {
            subj.get_observable()
        } -> rpp::constraint::observable;
        {
            subj.get_disposable()
        } -> rpp::constraint::decayed_any_of<rpp::disposable_wrapper, rpp::composite_disposable_wrapper>;
    };
} // namespace rpp::constraint

namespace rpp::subjects::utils
{
    template<constraint::subject T>
    using extract_subject_type_t = rpp::utils::extract_observer_type_t<decltype(std::declval<T>().get_observer())>;
} // namespace rpp::subjects::utils
