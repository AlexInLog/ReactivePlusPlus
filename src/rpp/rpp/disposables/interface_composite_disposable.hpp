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

#include <rpp/disposables/callback_disposable.hpp>
#include <rpp/disposables/interface_disposable.hpp>

namespace rpp
{
    struct interface_composite_disposable : public interface_disposable
    {
        virtual void add(disposable_wrapper disposable) = 0;

        template<rpp::constraint::is_nothrow_invocable Fn>
        disposable_wrapper add(Fn&& invocable)
        {
            auto d = make_callback_disposable(std::forward<Fn>(invocable));
            add(d);
            return d;
        }

        virtual void remove(const disposable_wrapper& d) = 0;
        // dispose all added disposables, clear container but not dispose original disposable
        virtual void clear() = 0;
    };
} // namespace rpp
