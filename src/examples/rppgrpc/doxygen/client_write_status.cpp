#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_interceptor.h>
#include <rppgrpc/rppgrpc.hpp>

#include "protocol.grpc.pb.h"

#include <chrono>
#include <cstdlib>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace
{
    constexpr auto rejection = "server rejected the call before reading a request";

    class rejecting_service final : public TestService::Service
    {
    public:
        std::future<void> headers_received;

    private:
        grpc::Status ClientSide(grpc::ServerContext* context, grpc::ServerReader<Request>* reader, Response* response) override
        {
            using namespace std::chrono_literals;
            if (context->client_metadata().contains("accept"))
            {
                Request request;
                while (reader->Read(&request))
                    response->set_value(request.value());
                return grpc::Status::OK;
            }
            if (context->client_metadata().contains("wait-for-headers"))
            {
                reader->SendInitialMetadata();
                if (headers_received.wait_for(2s) != std::future_status::ready)
                    return {grpc::StatusCode::INTERNAL, "client did not acknowledge initial metadata"};
            }
            return {grpc::StatusCode::ABORTED, rejection};
        }
    };

    template<typename Reactor>
    auto observe_terminal(Reactor& reactor)
    {
        const auto result = std::make_shared<std::promise<void>>();
        auto       done   = result->get_future();
        reactor.get_observable().subscribe(
            [](const auto&) { },
            [result](const std::exception_ptr& error) { result->set_exception(error); },
            [result] { result->set_value(); });
        return done;
    }

    template<typename Reactor>
    int expect_rejection(Reactor* reactor, std::string_view expected = rejection)
    {
        using namespace std::chrono_literals;

        auto done = observe_terminal(*reactor);
        reactor->init();

        // Keep the writer idle and open while the server rejects the call.
        if (done.wait_for(4s) != std::future_status::ready)
        {
            std::cerr << "Timed out waiting for the reactor to terminate\n";
            std::abort();
        }

        try
        {
            done.get();
            std::cerr << "Expected the RPC to fail\n";
            return 1;
        }
        catch (const rppgrpc::utils::reactor_failed& error)
        {
            std::cout << error.what() << '\n';
            if (std::string_view{error.what()} != expected)
                return 1;
        }

        return 0;
    }

    class headers_interceptor final : public grpc::experimental::Interceptor
    {
    public:
        explicit headers_interceptor(std::promise<void>& headers)
            : m_headers{headers}
        {
        }

        void Intercept(grpc::experimental::InterceptorBatchMethods* methods) override
        {
            if (methods->QueryInterceptionHookPoint(grpc::experimental::InterceptionHookPoints::POST_RECV_INITIAL_METADATA))
                m_headers.set_value();
            methods->Proceed();
        }

    private:
        std::promise<void>& m_headers;
    };

    class headers_factory final : public grpc::experimental::ClientInterceptorFactoryInterface
    {
    public:
        headers_factory(grpc::ClientContext& context, std::promise<void>& headers)
            : m_context{context}
            , m_headers{headers}
        {
        }

        grpc::experimental::Interceptor* CreateClientInterceptor(grpc::experimental::ClientRpcInfo* info) override
        {
            return info->client_context() == &m_context ? new headers_interceptor{m_headers} : nullptr;
        }

    private:
        grpc::ClientContext& m_context;
        std::promise<void>&  m_headers;
    };

    int run_status_interceptor_probe()
    {
        using namespace std::chrono_literals;

        grpc::ClientContext rejected_context;
        grpc::ClientContext accepted_context;
        std::promise<void>  headers;
        rejecting_service   service;
        service.headers_received = headers.get_future();

        grpc::ServerBuilder builder;
        builder.RegisterService(&service);
        const auto server = builder.BuildAndStart();
        if (!server)
            return 1;

        rppgrpc::client_status_hooks hooks;
        auto                         factories = hooks.make_interceptors();
        factories.push_back(std::make_unique<headers_factory>(rejected_context, headers));
        const auto stub = TestService::NewStub(server->experimental().InProcessChannelWithInterceptors({}, std::move(factories)));

        accepted_context.set_deadline(std::chrono::system_clock::now() + 5s);
        accepted_context.AddMetadata("accept", "1");
        Response accepted_response;
        auto*    accepted = new rppgrpc::client_write_reactor<Request>{accepted_context, hooks};
        stub->async()->ClientSide(&accepted_context, &accepted_response, accepted);
        const auto writer = accepted->get_observer();
        auto       done   = observe_terminal(*accepted);
        accepted->init();

        rejected_context.set_deadline(std::chrono::system_clock::now() + 3s);
        rejected_context.AddMetadata("wait-for-headers", "1");
        Response rejected_response;
        auto*    rejected = new rppgrpc::client_write_reactor<Request>{rejected_context, hooks};
        stub->async()->ClientSide(&rejected_context, &rejected_response, rejected);
        const auto rejected_writer = rejected->get_observer();
        const auto rejected_result = expect_rejection(rejected);
        rejected_writer.on_next(Request{});

        // The sibling call must still accept writes after the rejection.
        Request request;
        request.set_value("sent after sibling rejection");
        writer.on_next(request);
        writer.on_completed();
        if (done.wait_for(6s) != std::future_status::ready)
            std::abort();
        done.get();
        if (rejected_result != 0 || accepted_response.value() != request.value())
            return 1;

        const auto          plain_stub = TestService::NewStub(server->InProcessChannel({}));
        grpc::ClientContext plain_context;
        plain_context.set_deadline(std::chrono::system_clock::now() + 2s);
        Response plain_response;
        auto*    unmatched = new rppgrpc::client_write_reactor<Request>{plain_context, hooks};
        plain_stub->async()->ClientSide(&plain_context, &plain_response, unmatched);
        return expect_rejection(unmatched, "The client status interceptor did not bind this writer");
    }

} // namespace

int main()
{
    return run_status_interceptor_probe();
}
