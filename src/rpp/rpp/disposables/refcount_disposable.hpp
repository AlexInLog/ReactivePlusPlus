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

#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/disposables/details/base_disposable.hpp>
#include <rpp/disposables/disposable_wrapper.hpp>

#include <atomic>
#include <limits>

namespace rpp::details
{
    class refocunt_disposable_inner;
} // namespace rpp::details

namespace rpp
{
    class refcount_disposable : public rpp::details::enable_wrapper_from_this<refcount_disposable>
        , public rpp::composite_disposable
    {

        void release()
        {
            auto current_value = m_refcount.load(std::memory_order::seq_cst);
            while (current_value != s_disposed)
            {
                const size_t new_value = current_value == 1 ? s_disposed : current_value - 1;
                if (!m_refcount.compare_exchange_strong(current_value, new_value, std::memory_order::seq_cst))
                    continue;

                if (new_value == s_disposed)
                    dispose();
                return;
            }
        }

        void composite_dispose_impl(interface_disposable::Mode) noexcept override
        {
            m_refcount.store(s_disposed, std::memory_order::seq_cst);
        }

    public:
        friend class details::refocunt_disposable_inner;
        refcount_disposable() = default;

        composite_disposable_wrapper add_ref();

    private:
        std::atomic<size_t>     m_refcount{0};
        constexpr static size_t s_disposed = std::numeric_limits<size_t>::max();
    };
} // namespace rpp

namespace rpp::details
{
    class refocunt_disposable_inner final : public rpp::composite_disposable
        , public rpp::details::enable_wrapper_from_this<refocunt_disposable_inner>
    {
    public:
        refocunt_disposable_inner(disposable_wrapper_impl<refcount_disposable> state)
            : m_state{std::move(state)}
        {
        }

        void composite_dispose_impl(interface_disposable::Mode mode) noexcept override
        {
            if (mode != interface_disposable::Mode::Destroying)
                m_state.remove(this->wrapper_from_this());

            if (const auto locked = m_state.lock())
                locked->release();
            m_state = disposable_wrapper_impl<refcount_disposable>::empty();
        }

    private:
        disposable_wrapper_impl<refcount_disposable> m_state;
    };

} // namespace rpp::details

namespace rpp
{
    inline composite_disposable_wrapper refcount_disposable::add_ref()
    {
        auto current_value = m_refcount.load(std::memory_order::seq_cst);
        while (true)
        {
            if (current_value == s_disposed)
                return composite_disposable_wrapper::empty();

            // just need atomicity, not guarding anything
            if (m_refcount.compare_exchange_strong(current_value, current_value + 1, std::memory_order::seq_cst))
            {
                auto inner = composite_disposable_wrapper::make<details::refocunt_disposable_inner>(wrapper_from_this());
                add(inner.as_weak());
                return inner;
            }
        }
    }
} // namespace rpp
