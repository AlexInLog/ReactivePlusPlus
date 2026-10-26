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

#include <rpp/disposables/fwd.hpp>

#include <rpp/disposables/details/base_disposable.hpp>
#include <rpp/disposables/disposable_wrapper.hpp>

namespace rpp
{
    /**
     * @brief Disposable invokes underlying callable on disposing.
     *
     * @ingroup disposables
     */
    template<rpp::constraint::is_nothrow_invocable Fn>
    class callback_disposable final : public details::base_disposable
    {
    public:
        explicit callback_disposable(Fn&& fn)
            : m_fn{std::move(fn)}
        {
        }

        explicit callback_disposable(const Fn& fn)
            : m_fn{fn}
        {
        }

    private:
        void base_dispose_impl(interface_disposable::Mode) noexcept override { std::move(m_fn)(); } // NOLINT(bugprone-exception-escape)

    private:
        RPP_NO_UNIQUE_ADDRESS Fn m_fn;
    };

    template<rpp::constraint::is_nothrow_invocable Fn>
    disposable_wrapper make_callback_disposable(Fn&& invocable)
    {
        return disposable_wrapper::make<rpp::callback_disposable<std::decay_t<Fn>>>(std::forward<Fn>(invocable));
    }
} // namespace rpp
