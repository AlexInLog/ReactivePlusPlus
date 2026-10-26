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

#include <rpp/observers/details/fwd.hpp>

#include <rpp/defs.hpp>
#include <rpp/disposables/disposable_wrapper.hpp>

#include <atomic>

namespace rpp::details::observers
{
    template<typename DisposableContainer>
    class local_disposables_strategy
    {
    public:
        local_disposables_strategy()                                            = default;
        local_disposables_strategy(local_disposables_strategy&& other) noexcept = default;

        void add(const disposable_wrapper& d)
        {
            m_upstreams.push_back(d);
        }

        bool is_disposed() const noexcept
        {
            return m_is_disposed;
        }

        void dispose() const
        {
            m_is_disposed = true;
            m_upstreams.dispose();
        }

    private:
        RPP_NO_UNIQUE_ADDRESS DisposableContainer m_upstreams{};
        mutable bool                              m_is_disposed{};
    };

    struct none_disposables_strategy
    {
        static constexpr void add(const rpp::disposable_wrapper&) { }

        static constexpr bool is_disposed() noexcept { return false; }

        static constexpr void dispose() { }
    };

    class boolean_disposables_strategy
    {
    public:
        static constexpr void add(const rpp::disposable_wrapper&) { }

        bool is_disposed() const noexcept { return m_is_disposed; }

        void dispose() const { m_is_disposed = true; }

    private:
        mutable bool m_is_disposed{};
    };
} // namespace rpp::details::observers
