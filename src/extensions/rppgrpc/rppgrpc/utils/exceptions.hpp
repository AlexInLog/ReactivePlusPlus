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

namespace rppgrpc::utils
{
    struct reactor_failed : public std::runtime_error
    {
        using std::runtime_error::runtime_error;
    };

} // namespace rppgrpc::utils
