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

#include <stdexcept>

namespace rppqt::utils
{
    struct no_active_qapplication : std::runtime_error
    {
        using std::runtime_error::runtime_error;
    };
} // namespace rppqt::utils
