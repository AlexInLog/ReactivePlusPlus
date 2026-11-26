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

#include <rpp/observables/fwd.hpp>

#include <grpcpp/support/client_callback.h>
#include <rppgrpc/client_status_hooks.hpp>
#include <rppgrpc/details/base.hpp>
#include <rppgrpc/fwd.hpp>
#include <rppgrpc/utils/exceptions.hpp>

#include <optional>

namespace rppgrpc
{
    /**
     * @brief RPP's based implementation for grpc client bidirectional reactor.
     * @details To use it you need:
     * - create it via `new` operator
     * - pass it to `stub->async()->GrpcBidirectionalStream(ctx, reactor);`
     * - call `reactor->init()` method for actual starting of grpc logic
     * - to access values FROM stream you can subscribe to observable obtained via `reactor->get_observable()` (same observable WOULD emit on_completed in case of successful stream termination and on_error in case of some errors with grpc stream)
     * - to pass values TO stream you can emit values to observer obtained via `reactor->get_observer()`
     *
     * @warning After `init()` the call may end at any moment (connection refused, deadline, cancel) and the reactor deletes itself in `OnDone`, so obtain the observer and the observable before `init()` and do not use the reactor afterwards. The observer may be used from any thread, also after `init()`: the reactor holds the call (see `grpc::ClientBidiReactor::AddHold`) until its read side has ended; values still queued then, and values emitted after that, are dropped.
     *
     * @snippet client_reactor.cpp bidi_reactor
     *
     * @ingroup rppgrpc_reactors
     */
    template<rpp::constraint::decayed_type Request, rpp::constraint::decayed_type Response>
    class client_bidi_reactor final : public grpc::ClientBidiReactor<Request, Response>
        , private details::base_writer<Request>
        , private details::base_reader<Response>
    {
        using Base = grpc::ClientBidiReactor<Request, Response>;

    public:
        client_bidi_reactor() = default;

        void init()
        {
            // The observer may write from outside the reactions after StartCall(), which gRPC allows
            // only under a hold. OnReadDone(false) releases it.
            Base::AddHold();
            // The first read is posted before StartCall(): gRPC keeps it until the call starts and
            // counts it as an outstanding operation. StartCall() is the last use of the reactor in
            // init(), because a call that ends at once may run OnDone (and so `delete this`) on a
            // gRPC thread before StartCall() has even returned.
            details::base_reader<Response>::handle_read_done(true);
            Base::StartCall();
        }

        using details::base_writer<Request>::get_observer;
        using details::base_reader<Response>::get_observable;

    private:
        void start_read(Response& data) override
        {
            Base::StartRead(&data);
        }

        void start_write(const Request& v) override
        {
            Base::StartWrite(&v);
        }

        void finish_writes(const grpc::Status&) override
        {
            Base::StartWritesDone();
        }

        using Base::StartCall;
        using Base::StartRead;

        void OnReadDone(bool ok) override
        {
            if (!ok)
            {
                // One read is always posted, so each call ends its read side exactly once, here.
                // A write that took the observer's lock before close_writes() has called StartWrite
                // and is counted by gRPC, so OnDone comes after its OnWriteDone; later and still queued
                // writes are dropped. This reaction is itself still counted, so RemoveHold() does not run OnDone.
                details::base_writer<Request>::close_writes();
                Base::RemoveHold();
                return;
            }

            details::base_reader<Response>::handle_read_done();
        }

        void OnWriteDone(bool ok) override
        {
            if (!ok)
                return;

            details::base_writer<Request>::handle_write_done();
        }

        void OnDone(const grpc::Status& s) override
        {
            details::base_writer<Request>::handle_on_done();
            details::base_reader<Response>::handle_on_done(s.ok() ? std::exception_ptr{} : std::make_exception_ptr(rppgrpc::utils::reactor_failed{s.error_message()}));
            delete this;
        }
    };

    /**
     * @brief RPP's based implementation for grpc client write reactor
     * @details To use it you need:
     * - create it via `new` operator
     * - pass it to `stub->async()->GrpcWriteStream(ctx, &request, reactor);`
     * - call `reactor->init()` method for actual starting of grpc logic
     * - to pass values TO stream you can emit values to observer obtained via `reactor->get_observer()`
     * - reactor provides `reactor->get_observable()` method but such as observable emits nothing and can be used only to be notified about completion/error
     *
     * @warning Obtain the observer and observable before `init()`. The reactor may delete itself in `OnDone`.
     * With the default constructor, emit values before `init()`. For external writes after `init()`, construct the reactor
     * with the context used to bind the RPC and the hooks installed on the stub's channel. If the interceptor was
     * missing at binding, the call is cancelled and the observable reports a configuration error.
     *
     * @snippet client_reactor.cpp write_reactor
     *
     * @ingroup rppgrpc_reactors
     */
    template<rpp::constraint::decayed_type Request>
    class client_write_reactor final : public grpc::ClientWriteReactor<Request>
        , private details::base_writer<Request>
        , private details::base_reader<rpp::utils::none>
    {
        using Base = grpc::ClientWriteReactor<Request>;

    public:
        client_write_reactor() = default;

        client_write_reactor(grpc::ClientContext& context, const client_status_hooks& hooks)
            : m_registration{std::in_place, hooks, context, [this] {
                                 this->close_writes();
                                 // gRPC still counts its finish operation while the interceptor runs.
                                 Base::RemoveHold();
                             }}
        {
        }

        void init()
        {
            if (m_registration)
            {
                auto& context         = m_registration->context();
                m_missing_interceptor = !m_registration->claimed();
                m_registration.reset();
                if (m_missing_interceptor)
                {
                    this->close_writes();
                    context.TryCancel();
                }
                else
                {
                    Base::AddHold();
                }
            }
            Base::StartCall();
        }

        using details::base_writer<Request>::get_observer;
        using details::base_reader<rpp::utils::none>::get_observable;

    private:
        std::optional<client_status_hooks::registration> m_registration;
        bool                                             m_missing_interceptor{};

        void start_read(rpp::utils::none&) override
        {
        }

        void start_write(const Request& v) override
        {
            Base::StartWrite(&v);
        }

        void finish_writes(const grpc::Status&) override
        {
            Base::StartWritesDone();
        }

        using Base::StartCall;

        void OnWriteDone(bool ok) override
        {
            if (!ok)
                return;

            details::base_writer<Request>::handle_write_done();
        }

        void OnDone(const grpc::Status& s) override
        {
            details::base_writer<Request>::handle_on_done();
            if (m_missing_interceptor)
                details::base_reader<rpp::utils::none>::handle_on_done(std::make_exception_ptr(rppgrpc::utils::reactor_failed{"The client status interceptor did not bind this writer"}));
            else
                details::base_reader<rpp::utils::none>::handle_on_done(s.ok() ? std::exception_ptr{} : std::make_exception_ptr(rppgrpc::utils::reactor_failed{s.error_message()}));

            delete this;
        }
    };

    /**
     * @brief RPP's based implementation for grpc client read reactor.
     * @details To use it you need:
     * - create it via `new` operator
     * - pass it to `stub->async()->GrpcReadStream(ctx, &response, reactor);`
     * - call `reactor->init()` method for actual starting of grpc logic
     * - to access values FROM stream you can subscribe to observable obtained via `reactor->get_observable()` (same observable WOULD emit on_completed in case of successful stream termination and on_error in case of some errors with grpc stream)
     *
     * @snippet client_reactor.cpp read_reactor
     *
     * @ingroup rppgrpc_reactors
     */
    template<rpp::constraint::decayed_type Response>
    class client_read_reactor final : public grpc::ClientReadReactor<Response>
        , private details::base_reader<Response>
    {
        using Base = grpc::ClientReadReactor<Response>;

    public:
        client_read_reactor() = default;

        void init()
        {
            // The first read is posted before StartCall(): gRPC keeps it until the call starts and
            // counts it as an outstanding operation. StartCall() is the last use of the reactor in
            // init(), because a call that ends at once may run OnDone (and so `delete this`) on a
            // gRPC thread before StartCall() has even returned.
            details::base_reader<Response>::handle_read_done(true);
            Base::StartCall();
        }

        using details::base_reader<Response>::get_observable;

    private:
        using Base::StartCall;
        using Base::StartRead;

        void start_read(Response& data) override
        {
            Base::StartRead(&data);
        }

        void OnReadDone(bool ok) override
        {
            if (!ok)
                return;

            details::base_reader<Response>::handle_read_done();
        }

        void OnDone(const grpc::Status& s) override
        {
            details::base_reader<Response>::handle_on_done(s.ok() ? std::exception_ptr{} : std::make_exception_ptr(rppgrpc::utils::reactor_failed{s.error_message()}));
            delete this;
        }
    };
} // namespace rppgrpc
