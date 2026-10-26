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

#include <concepts>

class QObject;

namespace rppqt::source
{
    template<std::derived_from<QObject> TSignalQObject, std::derived_from<TSignalQObject> TObject, typename R, typename... Args>
    auto from_signal(const TObject& object, R (TSignalQObject::*signal)(Args...));
} // namespace rppqt::source
