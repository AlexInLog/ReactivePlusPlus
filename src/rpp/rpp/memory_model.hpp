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

#include <concepts>

namespace rpp::memory_model
{
    // copy and move everywhere when needed
    struct use_stack
    {
    };

    // make shared_ptr once and avoid any future copies/moves
    struct use_shared
    {
    };
} // namespace rpp::memory_model

namespace rpp::constraint
{
    template<typename T>
    concept memory_model = std::same_as<rpp::memory_model::use_shared, T> || std::same_as<rpp::memory_model::use_stack, T>;
} // namespace rpp::constraint
