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

#include <stdexcept>

namespace rpp::utils
{
    struct not_enough_emissions : public std::runtime_error
    {
        using std::runtime_error::runtime_error;
    };

    struct more_disposables_than_expected : public std::runtime_error
    {
        using std::runtime_error::runtime_error;
    };

    struct timeout_reached : public std::runtime_error
    {
        using std::runtime_error::runtime_error;
    };

    struct out_of_range : public std::range_error
    {
        using std::range_error::range_error;
    };
} // namespace rpp::utils
