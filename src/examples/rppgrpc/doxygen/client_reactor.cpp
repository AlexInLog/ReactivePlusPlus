#include <rpp/rpp.hpp>

#include <grpc++/create_channel.h>
#include <rppgrpc/rppgrpc.hpp>

#include "protocol.grpc.pb.h"
/**
 * @example client_reactor.cpp
 **/

int main() // NOLINT(bugprone-exception-escape)
{
    {
        //! [bidi_reactor]
        auto channel = grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials());
        auto stub    = TestService::NewStub(channel);

        grpc::ClientContext ctx{};
        const auto          reactor = new rppgrpc::client_bidi_reactor<Request, Response>();
        stub->async()->Bidirectional(&ctx, reactor);
        reactor->get_observable().subscribe([](const Response&) { });
        reactor->get_observer().on_next(Request{}); // posted until init() starts the call

        reactor->init();
        //! [bidi_reactor]
    }
    {
        //! [read_reactor]
        auto channel = grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials());
        auto stub    = TestService::NewStub(channel);

        grpc::ClientContext ctx{};
        const auto          reactor = new rppgrpc::client_read_reactor<Response>();
        Request             req{};
        stub->async()->ServerSide(&ctx, &req, reactor);
        reactor->get_observable().subscribe([](const Response&) { });

        reactor->init();
        //! [read_reactor]
    }
    {
        //! [write_reactor]
        auto channel = grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials());
        auto stub    = TestService::NewStub(channel);

        grpc::ClientContext ctx{};
        const auto          reactor = new rppgrpc::client_write_reactor<Request>();
        Response            resp{};
        stub->async()->ClientSide(&ctx, &resp, reactor);
        reactor->get_observable().subscribe([](const rpp::utils::none&) { });
        reactor->get_observer().on_next(Request{}); // posted until init() starts the call

        reactor->init();
        //! [write_reactor]
    }

    return 0;
}
