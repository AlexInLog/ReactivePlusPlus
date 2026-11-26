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

#include <grpcpp/client_context.h>
#include <grpcpp/support/client_interceptor.h>
#include <rppgrpc/fwd.hpp>

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace rppgrpc
{
    /** @brief Supplies the channel interceptors for external writes through client_write_reactor. */
    class client_status_hooks
    {
        using callback  = std::function<void()>;
        using factories = std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>>;

        struct registry
        {
            std::mutex                                mutex{};
            std::map<grpc::ClientContext*, callback*> pending{};
        };

        class status_interceptor final : public grpc::experimental::Interceptor
        {
        public:
            explicit status_interceptor(callback on_status)
                : m_on_status{std::move(on_status)}
            {
            }

            void Intercept(grpc::experimental::InterceptorBatchMethods* methods) override
            {
                if (methods->QueryInterceptionHookPoint(grpc::experimental::InterceptionHookPoints::POST_RECV_STATUS))
                    m_on_status();
                methods->Proceed();
            }

        private:
            callback m_on_status;
        };

        class factory final : public grpc::experimental::ClientInterceptorFactoryInterface
        {
        public:
            explicit factory(std::shared_ptr<registry> state)
                : m_state{std::move(state)}
            {
            }

            grpc::experimental::Interceptor* CreateClientInterceptor(grpc::experimental::ClientRpcInfo* info) override
            {
                std::lock_guard lock{m_state->mutex};
                auto            entry = m_state->pending.extract(info->client_context());
                if (entry.empty())
                    return nullptr;
                return new status_interceptor{std::exchange(*entry.mapped(), {})};
            }

        private:
            std::shared_ptr<registry> m_state;
        };

        std::shared_ptr<registry> m_state = std::make_shared<registry>();

        template<rpp::constraint::decayed_type Request>
        friend class client_write_reactor;

        class registration
        {
        public:
            registration(const client_status_hooks& hooks, grpc::ClientContext& context, callback on_status)
                : m_state{hooks.m_state}
                , m_context{context}
                , m_on_status{std::move(on_status)}
            {
                std::lock_guard lock{m_state->mutex};
                if (!m_state->pending.emplace(&m_context, &m_on_status).second)
                    throw std::logic_error{"A writer is already registered for this ClientContext"};
            }

            registration(const registration&)            = delete;
            registration& operator=(const registration&) = delete;

            ~registration()
            {
                std::lock_guard lock{m_state->mutex};
                if (m_on_status)
                    m_state->pending.erase(&m_context);
            }

            // The factory claims the callback synchronously during RPC binding.
            bool claimed() const { return !m_on_status; }

            grpc::ClientContext& context() const { return m_context; }

        private:
            std::shared_ptr<registry> m_state;
            grpc::ClientContext&      m_context;
            callback                  m_on_status;
        };

    public:
        factories make_interceptors() const
        {
            factories result;
            result.push_back(std::make_unique<factory>(m_state));
            return result;
        }
    };
} // namespace rppgrpc
