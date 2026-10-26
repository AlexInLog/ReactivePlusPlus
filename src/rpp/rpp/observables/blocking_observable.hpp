//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/observables/fwd.hpp>

#include <rpp/defs.hpp>
#include <rpp/operators/details/strategy.hpp>

#include <condition_variable>
#include <mutex>

namespace rpp::details::observables
{
    class blocking_disposable final : public base_disposable
    {
    public:
        void wait()
        {
            std::unique_lock lock{m_mutex};
            m_cv.wait(lock, [this] { return m_completed; });
        }

        void base_dispose_impl(interface_disposable::Mode) noexcept override
        {
            {
                std::lock_guard lock{m_mutex};
                m_completed = true;
            }
            m_cv.notify_all();
        }

    private:
        std::mutex              m_mutex{};
        std::condition_variable m_cv{};
        bool                    m_completed{};
    };

    template<rpp::constraint::decayed_type Type, rpp::constraint::observable_strategy<Type> Strategy>
    class blocking_strategy
    {
    public:
        using value_type                   = Type;
        using optimal_disposables_strategy = typename Strategy::optimal_disposables_strategy::template add<1>;

        blocking_strategy(observable<Type, Strategy>&& observable)
            : m_original{std::move(observable)}
        {
        }

        blocking_strategy(const observable<Type, Strategy>& observable)
            : m_original{observable}
        {
        }

        template<rpp::constraint::observer_strategy<Type> ObserverStrategy>
        void subscribe(observer<Type, ObserverStrategy>&& obs) const
        {
            auto d = disposable_wrapper_impl<blocking_disposable>::make();
            obs.set_upstream(d);
            m_original.subscribe(std::move(obs));

            if (!d.is_disposed())
                if (const auto locked = d.lock())
                    locked->wait();
        }

    private:
        RPP_NO_UNIQUE_ADDRESS observable<Type, Strategy> m_original;
    };
} // namespace rpp::details::observables

namespace rpp
{
    /**
     * @brief Extension over rpp::observable with set of blocking operators - it waits till completion of underlying observable
     *
     * @par Example:
     * @snippet as_blocking.cpp as_blocking
     *
     * @ingroup observables
     */
    template<constraint::decayed_type Type, constraint::observable_strategy<Type> Strategy>
    class blocking_observable : public observable<Type, details::observables::blocking_strategy<Type, Strategy>>
    {
    public:
        using observable<Type, details::observables::blocking_strategy<Type, Strategy>>::observable;
    };
} // namespace rpp
