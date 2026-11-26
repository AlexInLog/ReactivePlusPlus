#include <rpp/rpp.hpp>

#include <grpc++/create_channel.h>
#include <rppgrpc/rppgrpc.hpp>

#include "protocol.grpc.pb.h"

/**
 * @example server_reactor.cpp
 **/

class server : public TestService::CallbackService
{

    //! [read_reactor]
    grpc::ServerReadReactor<Request>* ClientSide(grpc::CallbackServerContext* /*context*/, Response* /*response*/) override
    {
        const auto reactor = new rppgrpc::server_read_reactor<Request>();
        reactor->get_observable().subscribe([](const Request&) { }, []() { std::cout << "DONE" << std::endl; });
        return reactor;
    }
    //! [read_reactor]

    //! [bidi_reactor]
    grpc::ServerBidiReactor<Request, Response>* Bidirectional(grpc::CallbackServerContext* context) override
    {
        const auto reactor = new rppgrpc::server_bidi_reactor<Request, Response>(context);
        reactor->get_observable().subscribe([](const Request&) { }, []() { std::cout << "DONE" << std::endl; });

        reactor->get_observer().on_next(Response{});

        return reactor;
    }
    //! [bidi_reactor]

    //! [write_reactor]
    grpc::ServerWriteReactor<Response>* ServerSide(grpc::CallbackServerContext* /*context*/, const Request* /*request*/) override
    {
        const auto reactor = new rppgrpc::server_write_reactor<Response>();
        reactor->get_observable().subscribe([](rpp::utils::none) { }, []() { std::cout << "DONE" << std::endl; });

        reactor->get_observer().on_next(Response{});

        return reactor;
    }
    //! [write_reactor]
};

int main()
{
    return 0;
}
