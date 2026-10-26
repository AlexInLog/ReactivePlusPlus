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

#include <rpp/disposables/details/container.hpp>
#include <rpp/disposables/disposable_wrapper.hpp>
#include <rpp/disposables/interface_composite_disposable.hpp>

#include <atomic>

namespace rpp
{
    /**
     * @brief Disposable which can keep some other sub-disposables. When this root disposable is disposed, then all sub-disposables would be disposed too.
     * @tparam Container is type of internal storage used to keep dependencies
     *
     * @ingroup disposables
     */
    template<details::disposables::constraint::disposables_container Container>
    class composite_disposable_impl : public interface_composite_disposable
    {
    public:
        composite_disposable_impl()                                           = default;
        composite_disposable_impl(const composite_disposable_impl&)           = delete;
        composite_disposable_impl(composite_disposable_impl&& other) noexcept = delete;

        bool is_disposed() const noexcept final
        {
            // just need atomicity, not guarding anything
            return m_current_state.load(std::memory_order::seq_cst) == State::Disposed;
        }

        void dispose_impl(interface_disposable::Mode mode) noexcept final
        {
            while (true)
            {
                State expected{State::None};
                // need to acquire possible state changing from `add`
                if (m_current_state.compare_exchange_strong(expected, State::Disposed, std::memory_order::seq_cst))
                {
                    composite_dispose_impl(mode);

                    m_disposables.dispose();
                    m_disposables.clear();
                    return;
                }

                if (expected == State::Disposed)
                    return;
            }
        }

        using interface_composite_disposable::add;

        void add(disposable_wrapper disposable) override
        {
            if (disposable.is_disposed() || disposable.lock().get() == this)
                return;

            while (true)
            {
                State expected{State::None};
                // need to acquire possible disposables state changing from other `add`
                if (m_current_state.compare_exchange_strong(expected, State::Edit, std::memory_order::seq_cst))
                {
                    try
                    {
                        m_disposables.push_back(std::move(disposable));
                    }
                    catch (...)
                    {
                        m_current_state.store(State::None, std::memory_order::seq_cst);
                        throw;
                    }
                    // need to propogate disposables state changing to others
                    m_current_state.store(State::None, std::memory_order::seq_cst);
                    return;
                }

                if (expected == State::Disposed)
                {
                    disposable.dispose();
                    return;
                }
            }
        }

        void remove(const disposable_wrapper& disposable) override
        {
            while (true)
            {
                State expected{State::None};
                // need to acquire possible disposables state changing from other `add` or `remove`
                if (m_current_state.compare_exchange_strong(expected, State::Edit, std::memory_order::seq_cst))
                {
                    try
                    {
                        m_disposables.remove(disposable);
                    }
                    catch (...)
                    {
                        m_current_state.store(State::None, std::memory_order::seq_cst);
                        throw;
                    }
                    // need to propogate disposables state changing to others
                    m_current_state.store(State::None, std::memory_order::seq_cst);
                    return;
                }

                if (expected == State::Disposed)
                    return;
            }
        }

        void clear() override
        {
            while (true)
            {
                State expected{State::None};
                // need to acquire possible disposables state changing from other `add` or `remove`
                if (m_current_state.compare_exchange_strong(expected, State::Edit, std::memory_order::seq_cst))
                {
                    try
                    {
                        m_disposables.dispose();
                        m_disposables.clear();
                    }
                    catch (...)
                    {
                        m_current_state.store(State::None, std::memory_order::seq_cst);
                        throw;
                    }
                    // need to propogate disposables state changing to others
                    m_current_state.store(State::None, std::memory_order::seq_cst);
                    return;
                }

                if (expected == State::Disposed)
                    return;
            }
        }

    protected:
        virtual void composite_dispose_impl(interface_disposable::Mode) noexcept { }

    private:
        enum class State : uint8_t
        {
            None,    // default state
            Edit,    // set it during adding new element into deps or removing. After success -> back to None
            Disposed // permanent state after dispose
        };

        Container          m_disposables{};
        std::atomic<State> m_current_state{};
    };

    /**
     * @brief Disposable which can keep some other sub-disposables. When this root disposable is disposed, then all sub-disposables would be disposed too.
     * @note By default uses vector as internal storage
     *
     * @ingroup disposables
     */
    class composite_disposable : public composite_disposable_impl<rpp::details::disposables::default_disposables_container>
    {
    };
} // namespace rpp
