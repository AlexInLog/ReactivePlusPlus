#include <doctest/doctest.h>

#include <rpp/observers/mock_observer.hpp>
#include <rpp/operators/map.hpp>
#include <rpp/operators/observe_on.hpp>

#include <grpc++/server_builder.h>
#include <rppgrpc/proto.grpc.pb.h>
#include <rppgrpc/proto.pb.h>
#include <rppgrpc/rppgrpc.hpp>

#include "rpp_trompeloil.hpp"


namespace
{
    struct service : public trompeloeil::mock_interface<TestService::CallbackService>
    {
        using write_reactor_ptr = grpc::ServerWriteReactor<Response>*;
        using read_reactor_ptr  = grpc::ServerReadReactor<Request>*;
        using bidi_reactor_ptr  = grpc::ServerBidiReactor<Request, Response>*;

        MAKE_MOCK2(ServerSide, write_reactor_ptr(grpc::CallbackServerContext* /*context*/, const Request* /*request*/));
        MAKE_MOCK2(ClientSide, read_reactor_ptr(grpc::CallbackServerContext* /*context*/, Response* /*response*/));
        MAKE_MOCK1(Bidirectional, bidi_reactor_ptr(grpc::CallbackServerContext* /*context*/));
    };
} // namespace


TEST_CASE("Async server")
{
    grpc::ServerBuilder   builder{};
    trompeloeil::sequence s{};

    auto mock_service = std::make_unique<service>();

    builder.RegisterService(mock_service.get());

    auto server(builder.BuildAndStart());

    const auto channel = server->InProcessChannel({});
    const auto stub    = TestService::NewStub(channel, {});

    mock_observer<uint32_t>      out_mock{};
    grpc::ClientContext          ctx{};
    grpc::CallbackServerContext* obtained_context{};

    auto test_common = [&](const auto& writer, const auto* reactor) {
        SUBCASE("writer immediate finish")
        {
            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_completed()).IN_SEQUENCE(s);
            if constexpr (requires { writer->WritesDone(); })
                writer->WritesDone();

            if constexpr (requires { reactor->get_observer(); })
                reactor->get_observer().on_completed();

            const auto res = writer->Finish().ok();
            CHECK(res);
            wait(last);
        }

        SUBCASE("writer cancels")
        {
            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);
            ctx.TryCancel();
            wait(last);
        }

        SUBCASE("server cancels")
        {
            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);
            obtained_context->TryCancel();
            wait(last);
        }
    };

    auto test_read = [&](const auto& writer, const auto* reactor) {
        SUBCASE("writer writes")
        {
            REQUIRE_CALL(*out_mock, on_next_rvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*out_mock, on_next_rvalue(2)).IN_SEQUENCE(s);
            REQUIRE_CALL(*out_mock, on_next_rvalue(3)).IN_SEQUENCE(s);
            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_completed()).IN_SEQUENCE(s);
            Request    request{};
            for (int i : {1, 2, 3})
            {
                request.set_value(i);
                REQUIRE(writer->Write(request));
            }
            writer->WritesDone();

            if constexpr (requires { reactor->get_observer(); })
                reactor->get_observer().on_completed();

            const auto res = writer->Finish().ok();
            REQUIRE(res);

            wait(last);
        }
    };

    auto test_write = [&](const auto& writer, const auto* reactor) {
        SUBCASE("writer reads")
        {
            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_completed()).IN_SEQUENCE(s);
            Response   response{};
            for (int i : {1, 2, 3})
            {
                response.set_value(i);
                reactor->get_observer().on_next(response);
            }
            reactor->get_observer().on_completed();

            if constexpr (requires { writer->WritesDone(); })
                writer->WritesDone();

            for (int i : {1, 2, 3})
            {
                REQUIRE(writer->Read(&response));
                REQUIRE(response.value() == i);
            }
            REQUIRE(!writer->Read(&response));
            const auto res = writer->Finish().ok();
            REQUIRE(res);
            wait(last);
        }
    };

    SUBCASE("bidirectionl")
    {
        const auto reactor = new rppgrpc::server_bidi_reactor<Request, Response>();
        reactor->get_observable() | rpp::ops::map([](const Request& out) { return out.value(); }) | rpp::ops::subscribe(out_mock);

        const auto call = NAMED_REQUIRE_CALL(*mock_service, Bidirectional(trompeloeil::_)).LR_SIDE_EFFECT(obtained_context = _1;).RETURN(reactor).IN_SEQUENCE(s);

        const auto writer = stub->Bidirectional(&ctx);

        wait(call);

        test_common(writer, reactor);
        test_read(writer, reactor);
        test_write(writer, reactor);
    }

    SUBCASE("server-side")
    {
        const auto reactor = new rppgrpc::server_write_reactor<Response>();
        reactor->get_observable() | rpp::ops::map([](const rpp::utils::none&) { return 0; }) | rpp::ops::subscribe(out_mock);

        const auto call = NAMED_REQUIRE_CALL(*mock_service, ServerSide(trompeloeil::_, trompeloeil::_)).LR_SIDE_EFFECT(obtained_context = _1;).RETURN(reactor).IN_SEQUENCE(s);

        const auto writer = stub->ServerSide(&ctx, {});

        wait(call);

        test_common(writer, reactor);
        test_write(writer, reactor);
    }

    SUBCASE("server-side when the reader cancels during a write")
    {
        // The cancel fails the write in flight and OnCancel() follows: one Finish per call.
        const auto make_reactor = [] {
            const auto reactor = new rppgrpc::server_write_reactor<Response>();
            Response   response{};
            for (uint32_t i = 0; i < 50; ++i)
            {
                response.set_value(i);
                reactor->get_observer().on_next(response);
            }
            return reactor;
        };

        const auto call = NAMED_ALLOW_CALL(*mock_service, ServerSide(trompeloeil::_, trompeloeil::_)).LR_RETURN(make_reactor());

        for (int i = 0; i < 200; ++i)
        {
            grpc::ClientContext context{};
            const auto          reader = stub->ServerSide(&context, {});
            Response            response{};
            REQUIRE(reader->Read(&response));
            context.TryCancel();
            CHECK(reader->Finish().error_code() == grpc::StatusCode::CANCELLED);
        }
    }

    SUBCASE("client-side")
    {
        const auto reactor = new rppgrpc::server_read_reactor<Request>();
        reactor->get_observable() | rpp::ops::map([](const Request& out) { return out.value(); }) | rpp::ops::subscribe(out_mock);

        const auto call = NAMED_REQUIRE_CALL(*mock_service, ClientSide(trompeloeil::_, trompeloeil::_)).LR_SIDE_EFFECT(obtained_context = _1;).RETURN(reactor).IN_SEQUENCE(s);

        Response   response{};
        const auto writer = stub->ClientSide(&ctx, &response);

        wait(call);

        test_common(writer, reactor);
        test_read(writer, reactor);
    }

    server->Shutdown();
    server->Wait();
}
