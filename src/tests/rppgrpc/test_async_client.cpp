#include <doctest/doctest.h>

#include <rpp/observers/mock_observer.hpp>
#include <rpp/operators/map.hpp>
#include <rpp/operators/observe_on.hpp>
#include <rpp/schedulers/new_thread.hpp>
#include <rpp/subjects/publish_subject.hpp>

#include <grpc++/create_channel.h>
#include <grpc++/server_builder.h>
#include <rppgrpc/proto.grpc.pb.h>
#include <rppgrpc/proto.pb.h>
#include <rppgrpc/rppgrpc.hpp>

#include "rpp_trompeloil.hpp"

#include <atomic>
#include <condition_variable>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

namespace
{
    struct service : public trompeloeil::mock_interface<TestService::Service>
    {
        IMPLEMENT_MOCK3(ServerSide);
        IMPLEMENT_MOCK3(ClientSide);
        IMPLEMENT_MOCK2(Bidirectional);
    };

    // The write side of the reactors, with a start_write that signals `entered` and waits for `release`.
    struct blocking_writer final : rppgrpc::details::base_writer<int>
    {
        using base_writer::handle_on_done;

        blocking_writer(std::promise<void>& entered, std::shared_future<void> release)
            : m_entered{entered}
            , m_release{std::move(release)}
        {
        }

    private:
        void start_write(const int&) override
        {
            m_entered.set_value();
            m_release.wait();
        }

        // The test emits no terminal, so the write side is never finished.
        void finish_writes(const grpc::Status&) override { }

        std::promise<void>&      m_entered;
        std::shared_future<void> m_release;
    };

    // The write side of the reactors, recording what it passes to the stream.
    struct recording_writer final : rppgrpc::details::base_writer<int>
    {
        using base_writer::close_writes;
        using base_writer::handle_write_done;

        std::vector<int> writes{};
        int              finishes{};

    private:
        void start_write(const int& v) override { writes.push_back(v); }
        void finish_writes(const grpc::Status&) override { ++finishes; }
    };

    // Threads that live as long as this object. start() hands them an observer, and each of them emits up to
    // `writes` values into it until stop() is called. stop() returns once every thread has left the observer.
    class writer_threads
    {
    public:
        explicit writer_threads(size_t count)
        {
            for (size_t i = 0; i < count; ++i)
                m_threads.emplace_back([this] { run(); });
        }

        writer_threads(const writer_threads&)            = delete;
        writer_threads& operator=(const writer_threads&) = delete;

        ~writer_threads()
        {
            {
                std::lock_guard lock{m_mutex};
                m_exit = true;
            }
            m_cv.notify_all();
            for (auto& t : m_threads)
                t.join();
        }

        void start(rpp::dynamic_observer<Request> observer, uint32_t writes)
        {
            {
                std::lock_guard lock{m_mutex};
                m_observer.emplace(std::move(observer));
                m_writes  = writes;
                m_stop    = false;
                m_running = m_threads.size();
                ++m_round;
            }
            m_cv.notify_all();
        }

        void stop()
        {
            m_stop = true;
            std::unique_lock lock{m_mutex};
            m_cv.wait(lock, [this] { return m_running == 0; });
            m_observer.reset();
        }

    private:
        void run()
        {
            size_t round = 0;
            while (true)
            {
                const rpp::dynamic_observer<Request>* observer{};
                uint32_t                              writes{};
                {
                    std::unique_lock lock{m_mutex};
                    m_cv.wait(lock, [&] { return m_exit || m_round != round; });
                    if (m_exit)
                        return;
                    round    = m_round;
                    observer = &*m_observer;
                    writes   = m_writes;
                }

                Request request{};
                for (uint32_t v = 0; v < writes && !m_stop; ++v)
                {
                    request.set_value(v);
                    observer->on_next(request);
                }

                {
                    std::lock_guard lock{m_mutex};
                    --m_running;
                }
                m_cv.notify_all();
            }
        }

        std::mutex                                    m_mutex{};
        std::condition_variable                       m_cv{};
        std::optional<rpp::dynamic_observer<Request>> m_observer{};
        uint32_t                                      m_writes{};
        size_t                                        m_round{};
        size_t                                        m_running{};
        bool                                          m_exit{};
        std::atomic_bool                              m_stop{};
        std::vector<std::thread>                      m_threads{};
    };
} // namespace

TEST_CASE("async client reactor")
{
    grpc::ServerBuilder   builder{};
    trompeloeil::sequence s{};

    auto mock_service = std::make_unique<service>();

    builder.RegisterService(mock_service.get());

    auto       server(builder.BuildAndStart());
    const auto channel = server->InProcessChannel({});

    const auto stub = TestService::NewStub(channel, {});

    rpp::subjects::publish_subject<uint32_t> subj{};
    mock_observer<uint32_t>                  out_mock{};

    SUBCASE("bidirectional")
    {
        grpc::ClientContext ctx{};

        const auto bidi_reactor = new rppgrpc::client_bidi_reactor<Request, Response>();
        bidi_reactor->get_observable() | rpp::ops::map([](const Response& out) { return out.value(); }) | rpp::ops::observe_on(rpp::schedulers::new_thread{}) | rpp::ops::subscribe(out_mock);
        subj.get_observable() | rpp::ops::map([](int v) { Request request{}; request.set_value(v); return request; }) | rpp::ops::subscribe(bidi_reactor->get_observer());

        stub->async()->Bidirectional(&ctx, bidi_reactor);
        SUBCASE("no stream job - completion")
        {
            REQUIRE_CALL(*mock_service, Bidirectional(trompeloeil::_, trompeloeil::_)).RETURN(grpc::Status::OK).IN_SEQUENCE(s);
            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_completed()).IN_SEQUENCE(s);
            bidi_reactor->init();

            wait(last);
        }

        SUBCASE("error status - error")
        {
            REQUIRE_CALL(*mock_service, Bidirectional(trompeloeil::_, trompeloeil::_)).RETURN(grpc::Status::CANCELLED).IN_SEQUENCE(s);
            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);
            bidi_reactor->init();

            wait(last);
        }

        SUBCASE("manual server-side cancel")
        {
            REQUIRE_CALL(*mock_service, Bidirectional(trompeloeil::_, trompeloeil::_))
                .RETURN(grpc::Status::OK)
                .LR_SIDE_EFFECT({
                    ctx.TryCancel();
                });


            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);

            bidi_reactor->init();

            wait(last);
        }

        SUBCASE("manual client-side completion")
        {
            REQUIRE_CALL(*mock_service, Bidirectional(trompeloeil::_, trompeloeil::_))
                .RETURN(grpc::Status::OK)
                .LR_SIDE_EFFECT({
                    Request request{};
                    while (_2->Read(&request))
                    {
                    }
                });


            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_completed()).IN_SEQUENCE(s);
            subj.get_observer().on_completed();

            bidi_reactor->init();

            wait(last);
        }

        SUBCASE("client-side write + completion")
        {
            std::promise<std::vector<int>> results{};
            REQUIRE_CALL(*mock_service, Bidirectional(trompeloeil::_, trompeloeil::_))
                .RETURN(grpc::Status::OK)
                .LR_SIDE_EFFECT({
                    std::vector<int> reads{};
                    Request          request{};
                    while (_2->Read(&request))
                    {
                        reads.push_back(request.value());
                    }
                    results.set_value(reads);
                });


            subj.get_observer().on_next(1);
            subj.get_observer().on_next(2);

            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_completed()).IN_SEQUENCE(s);
            subj.get_observer().on_completed();

            bidi_reactor->init();

            auto f = results.get_future();
            REQUIRE(f.wait_for(std::chrono::seconds{1}) == std::future_status::ready);
            CHECK(f.get() == std::vector<int>{1, 2});

            wait(last);
        }

        SUBCASE("client-side read + completion")
        {
            REQUIRE_CALL(*mock_service, Bidirectional(trompeloeil::_, trompeloeil::_))
                .RETURN(grpc::Status::OK)
                .LR_SIDE_EFFECT({
                    Response response{};

                    for (int v : {1, 2, 3})
                    {
                        response.set_value(v);
                        _2->Write(response);
                    }
                });

            REQUIRE_CALL(*out_mock, on_next_rvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*out_mock, on_next_rvalue(2)).IN_SEQUENCE(s);
            REQUIRE_CALL(*out_mock, on_next_rvalue(3)).IN_SEQUENCE(s);
            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_completed()).IN_SEQUENCE(s);

            bidi_reactor->init();

            wait(last);
        }

        SUBCASE("client-side read-write + completeion")
        {
            REQUIRE_CALL(*mock_service, Bidirectional(trompeloeil::_, trompeloeil::_))
                .RETURN(grpc::Status::OK)
                .LR_SIDE_EFFECT({
                    Request request{};
                    while (_2->Read(&request))
                    {
                        Response response{};
                        response.set_value(request.value() * 10);
                        _2->Write(response);
                    }
                });


            REQUIRE_CALL(*out_mock, on_next_rvalue(10)).IN_SEQUENCE(s);
            subj.get_observer().on_next(1);

            REQUIRE_CALL(*out_mock, on_next_rvalue(20)).IN_SEQUENCE(s);
            subj.get_observer().on_next(2);

            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_completed()).IN_SEQUENCE(s);
            subj.get_observer().on_completed();

            bidi_reactor->init();

            wait(last);
        }
    }
    SUBCASE("server-side")
    {
        grpc::ClientContext ctx{};

        const auto read_reactor = new rppgrpc::client_read_reactor<Response>();
        read_reactor->get_observable() | rpp::ops::map([](const Response& out) { return out.value(); }) | rpp::ops::observe_on(rpp::schedulers::new_thread{}) | rpp::ops::subscribe(out_mock);

        SUBCASE("empty request")
        {
            stub->async()->ServerSide(&ctx, nullptr, read_reactor);

            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);
            read_reactor->init();

            wait(last);
        }
        SUBCASE("normal request")
        {
            Request r{};
            stub->async()->ServerSide(&ctx, &r, read_reactor);

            SUBCASE("no stream job - completion")
            {
                REQUIRE_CALL(*mock_service, ServerSide(trompeloeil::_, trompeloeil::_, trompeloeil::_)).RETURN(grpc::Status::OK).IN_SEQUENCE(s);
                const auto last = NAMED_REQUIRE_CALL(*out_mock, on_completed()).IN_SEQUENCE(s);
                read_reactor->init();

                wait(last);
            }

            SUBCASE("error status - error")
            {
                REQUIRE_CALL(*mock_service, ServerSide(trompeloeil::_, trompeloeil::_, trompeloeil::_)).RETURN(grpc::Status::CANCELLED).IN_SEQUENCE(s);
                const auto last = NAMED_REQUIRE_CALL(*out_mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);
                read_reactor->init();

                wait(last);
            }

            SUBCASE("manual server-side cancel")
            {
                REQUIRE_CALL(*mock_service, ServerSide(trompeloeil::_, trompeloeil::_, trompeloeil::_))
                    .RETURN(grpc::Status::OK)
                    .LR_SIDE_EFFECT({
                        ctx.TryCancel();
                    });


                const auto last = NAMED_REQUIRE_CALL(*out_mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);

                read_reactor->init();

                wait(last);
            }

            SUBCASE("client-side read + completion")
            {
                REQUIRE_CALL(*mock_service, ServerSide(trompeloeil::_, trompeloeil::_, trompeloeil::_))
                    .RETURN(grpc::Status::OK)
                    .LR_SIDE_EFFECT({
                        Response response{};

                        for (int v : {1, 2, 3})
                        {
                            response.set_value(v);
                            _3->Write(response);
                        }
                    });

                REQUIRE_CALL(*out_mock, on_next_rvalue(1)).IN_SEQUENCE(s);
                REQUIRE_CALL(*out_mock, on_next_rvalue(2)).IN_SEQUENCE(s);
                REQUIRE_CALL(*out_mock, on_next_rvalue(3)).IN_SEQUENCE(s);
                const auto last = NAMED_REQUIRE_CALL(*out_mock, on_completed()).IN_SEQUENCE(s);

                read_reactor->init();

                wait(last);
            }
        }
    }
    SUBCASE("client-side")
    {
        grpc::ClientContext ctx{};

        const auto write_reactor = new rppgrpc::client_write_reactor<Request>();
        write_reactor->get_observable() | rpp::ops::map([](const rpp::utils::none& out) { return 0; }) | rpp::ops::observe_on(rpp::schedulers::new_thread{}) | rpp::ops::subscribe(out_mock);
        subj.get_observable() | rpp::ops::map([](int v) { Request request{}; request.set_value(v); return request; }) | rpp::ops::subscribe(write_reactor->get_observer());

        Response r{};
        stub->async()->ClientSide(&ctx, &r, write_reactor);
        SUBCASE("no stream job - completion")
        {
            REQUIRE_CALL(*mock_service, ClientSide(trompeloeil::_, trompeloeil::_, trompeloeil::_)).RETURN(grpc::Status::OK).IN_SEQUENCE(s);
            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_completed()).IN_SEQUENCE(s);
            write_reactor->init();

            wait(last);
        }

        SUBCASE("error status - error")
        {
            REQUIRE_CALL(*mock_service, ClientSide(trompeloeil::_, trompeloeil::_, trompeloeil::_)).RETURN(grpc::Status::CANCELLED).IN_SEQUENCE(s);
            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);
            write_reactor->init();

            wait(last);
        }

        SUBCASE("manual server-side cancel")
        {
            REQUIRE_CALL(*mock_service, ClientSide(trompeloeil::_, trompeloeil::_, trompeloeil::_))
                .RETURN(grpc::Status::OK)
                .LR_SIDE_EFFECT({
                    ctx.TryCancel();
                });


            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);

            write_reactor->init();

            wait(last);
        }

        SUBCASE("manual client-side completion")
        {
            REQUIRE_CALL(*mock_service, ClientSide(trompeloeil::_, trompeloeil::_, trompeloeil::_))
                .RETURN(grpc::Status::OK)
                .LR_SIDE_EFFECT({
                    Request request{};
                    while (_2->Read(&request))
                    {
                    }
                });


            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_completed()).IN_SEQUENCE(s);
            subj.get_observer().on_completed();

            write_reactor->init();

            wait(last);
        }

        SUBCASE("client-side write + completion")
        {
            std::promise<std::vector<int>> results{};
            REQUIRE_CALL(*mock_service, ClientSide(trompeloeil::_, trompeloeil::_, trompeloeil::_))
                .RETURN(grpc::Status::OK)
                .LR_SIDE_EFFECT({
                    std::vector<int> reads{};
                    Request          request{};
                    while (_2->Read(&request))
                    {
                        reads.push_back(request.value());
                    }
                    results.set_value(reads);
                });


            subj.get_observer().on_next(1);
            subj.get_observer().on_next(2);

            const auto last = NAMED_REQUIRE_CALL(*out_mock, on_completed()).IN_SEQUENCE(s);
            subj.get_observer().on_completed();

            write_reactor->init();

            auto f = results.get_future();
            REQUIRE(f.wait_for(std::chrono::seconds{1}) == std::future_status::ready);
            CHECK(f.get() == std::vector<int>{1, 2});

            wait(last);
        }
    }
    server->Shutdown();
    server->Wait();
}

TEST_CASE("async client reactor: call that ends during init()")
{
    // The context is cancelled before init(), so the call ends as soon as StartCall() is
    // issued, and gRPC may run OnDone (which deletes the reactor) on one of its own threads
    // while init() has not returned yet. init() must not use the reactor after StartCall().
    // A violation is a use-after-free; AddressSanitizer reports it.
    constexpr int iterations = 300;

    grpc::ServerBuilder builder{};
    auto                mock_service = std::make_unique<service>();
    ALLOW_CALL(*mock_service, ServerSide(trompeloeil::_, trompeloeil::_, trompeloeil::_)).RETURN(grpc::Status::CANCELLED);
    ALLOW_CALL(*mock_service, ClientSide(trompeloeil::_, trompeloeil::_, trompeloeil::_)).RETURN(grpc::Status::CANCELLED);
    ALLOW_CALL(*mock_service, Bidirectional(trompeloeil::_, trompeloeil::_)).RETURN(grpc::Status::CANCELLED);
    builder.RegisterService(mock_service.get());

    auto       server = builder.BuildAndStart();
    const auto stub   = TestService::NewStub(server->InProcessChannel({}), {});

    // Starts one call with an already cancelled context and waits for the reactor's terminal,
    // which it emits from OnDone. Context and request outlive the call.
    const auto run_call = [](auto* reactor, auto&& bind) {
        grpc::ClientContext ctx{};
        Request             request{};
        Response            response{};
        const auto          done = std::make_shared<std::promise<void>>();
        reactor->get_observable().subscribe([](const auto&) { },
                                            [done](const std::exception_ptr&) { done->set_value(); },
                                            [done] { done->set_value(); });
        bind(ctx, request, response, reactor);
        ctx.TryCancel();
        reactor->init();
        return done->get_future().wait_for(std::chrono::seconds{5}) == std::future_status::ready;
    };

    SUBCASE("bidirectional")
    {
        for (int i = 0; i < iterations; ++i)
            REQUIRE(run_call(new rppgrpc::client_bidi_reactor<Request, Response>(), [&](auto& ctx, auto&, auto&, auto* r) { stub->async()->Bidirectional(&ctx, r); }));
    }
    SUBCASE("server-side")
    {
        for (int i = 0; i < iterations; ++i)
            REQUIRE(run_call(new rppgrpc::client_read_reactor<Response>(), [&](auto& ctx, auto& req, auto&, auto* r) { stub->async()->ServerSide(&ctx, &req, r); }));
    }
    SUBCASE("client-side")
    {
        for (int i = 0; i < iterations; ++i)
            REQUIRE(run_call(new rppgrpc::client_write_reactor<Request>(), [&](auto& ctx, auto&, auto& resp, auto* r) { stub->async()->ClientSide(&ctx, &resp, r); }));
    }

    server->Shutdown();
    server->Wait();
}

TEST_CASE("async client reactor: write that is under way when the call ends")
{
    // t0 is inside start_write. t1 has entered the observer and waits behind t0. Then the write side ends
    // and its owner is destroyed, as in OnDone. Ending the write side waits until t0 has left start_write,
    // and neither thread may touch the destroyed owner afterwards: a write that does is a use-after-free;
    // AddressSanitizer reports it.
    std::promise<void> entered{};
    std::promise<void> release{};
    auto               writer   = std::make_unique<blocking_writer>(entered, release.get_future().share());
    const auto         observer = writer->get_observer();

    std::thread t0{[&] { observer.on_next(1); }};
    entered.get_future().wait();
    std::thread t1{[&] { observer.on_next(2); }};
    std::this_thread::sleep_for(std::chrono::milliseconds{50}); // t1 waits behind t0

    std::atomic_bool closed{};
    auto             done = std::async(std::launch::async, [&] {
        writer->handle_on_done();
        closed = true;
        writer.reset();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds{50}); // handle_on_done() runs while t0 is inside start_write
    CHECK_FALSE(closed.load());                                 // and has not returned
    release.set_value();

    done.get();
    CHECK(closed.load());
    t0.join();
    t1.join();
}

TEST_CASE("async client reactor: values still queued when the write side closes are dropped")
{
    // A reactor closes its write side once its stream has ended (client_bidi_reactor in OnReadDone(false)).
    // The completion of the write that was in flight then starts neither the next queued value nor the
    // half-close.
    recording_writer writer{};
    const auto       observer = writer.get_observer();
    observer.on_next(1);
    observer.on_next(2);
    observer.on_completed();

    writer.close_writes();
    writer.handle_write_done(); // OnWriteDone(true) for the value 1

    CHECK(writer.writes == std::vector<int>{1});
    CHECK(writer.finishes == 0);
}

TEST_CASE("async client reactor: writes from other threads while the call ends")
{
    // Threads emit into the reactor's observer after init() while the call ends. A write that reaches the
    // reactor after OnDone, or its stream after gRPC has destroyed it, is a use-after-free; AddressSanitizer
    // reports it.
    constexpr int      iterations = 200;
    constexpr size_t   writers    = 2;
    constexpr uint32_t writes     = 1000;

    grpc::ServerBuilder builder{};
    auto                mock_service = std::make_unique<service>();
    int                 server_reads = 0; // the server returns OK after this many reads, or when the client's side ends
    ALLOW_CALL(*mock_service, Bidirectional(trompeloeil::_, trompeloeil::_))
        .RETURN(grpc::Status::OK)
        .LR_SIDE_EFFECT({
            Request request{};
            for (int i = 0; i < server_reads && _2->Read(&request); ++i)
            {
            }
        });
    builder.RegisterService(mock_service.get());

    auto       server = builder.BuildAndStart();
    const auto stub   = TestService::NewStub(server->InProcessChannel({}), {});

    // The same `writers` threads serve every call of a subcase.
    writer_threads threads{writers};

    // Starts one call, lets the writer threads emit into its observer, ends the call with `end_call` and waits for
    // the reactor's terminal, which it emits from OnDone. The context must outlive the call: after a timeout the
    // call is cancelled and OnDone is still awaited (a call that never ends is left to the process timeout).
    const auto run_call = [&](const auto& end_call) {
        grpc::ClientContext ctx{};
        const auto          done    = std::make_shared<std::promise<void>>();
        const auto          reactor = new rppgrpc::client_bidi_reactor<Request, Response>();
        reactor->get_observable().subscribe([](const Response&) { },
                                            [done](const std::exception_ptr&) { done->set_value(); },
                                            [done] { done->set_value(); });
        stub->async()->Bidirectional(&ctx, reactor);
        auto observer = reactor->get_observer().as_dynamic();
        reactor->init();

        threads.start(std::move(observer), writes);
        end_call(ctx);
        auto       on_done = done->get_future();
        const bool ended   = on_done.wait_for(std::chrono::seconds{5}) == std::future_status::ready;
        threads.stop();
        if (!ended)
        {
            ctx.TryCancel();
            on_done.wait();
        }
        return ended;
    };

    SUBCASE("server finishes the call")
    {
        server_reads = 10;
        for (int i = 0; i < iterations; ++i)
            REQUIRE(run_call([](grpc::ClientContext&) { }));
    }
    SUBCASE("client cancels the call")
    {
        server_reads = std::numeric_limits<int>::max();
        for (int i = 0; i < iterations; ++i)
            REQUIRE(run_call([](grpc::ClientContext& ctx) { ctx.TryCancel(); }));
    }

    server->Shutdown();
    server->Wait();
}

TEST_CASE("async client reactor: bidirectional call reaches OnDone however it ends")
{
    // Each call gets a write from outside the reactions after init(), and must still reach OnDone, which emits
    // the observable's terminal and deletes the reactor (LeakSanitizer reports a reactor that is never deleted).
    // A context cancelled before init() is covered by "call that ends during init()".
    grpc::ServerBuilder builder{};
    auto                mock_service  = std::make_unique<service>();
    grpc::Status        server_status = grpc::Status::OK;
    bool                server_reads  = false; // the server reads until the client's side ends, then returns
    ALLOW_CALL(*mock_service, Bidirectional(trompeloeil::_, trompeloeil::_))
        .LR_RETURN(server_status)
        .LR_SIDE_EFFECT({
            Request request{};
            while (server_reads && _2->Read(&request))
            {
            }
        });
    builder.RegisterService(mock_service.get());

    auto                           server  = builder.BuildAndStart();
    std::shared_ptr<grpc::Channel> channel = server->InProcessChannel({});
    grpc::ClientContext            ctx{};

    enum class terminal
    {
        none,
        completed,
        error
    };

    // Starts one call on `channel`, writes once after init(), ends the call with `end_call` and returns the
    // terminal the reactor emitted from OnDone. The context must outlive the call: after a timeout the call is
    // cancelled and OnDone is still awaited (a call that never ends is left to the process timeout).
    const auto run_call = [&](const auto& end_call) {
        const auto stub    = TestService::NewStub(channel, {});
        const auto result  = std::make_shared<std::promise<terminal>>();
        const auto reactor = new rppgrpc::client_bidi_reactor<Request, Response>();
        reactor->get_observable().subscribe([](const Response&) { },
                                            [result](const std::exception_ptr&) { result->set_value(terminal::error); },
                                            [result] { result->set_value(terminal::completed); });
        stub->async()->Bidirectional(&ctx, reactor);
        const auto observer = reactor->get_observer();
        reactor->init();
        observer.on_next(Request{});
        end_call();
        auto f = result->get_future();
        if (f.wait_for(std::chrono::seconds{5}) == std::future_status::ready)
            return f.get();
        ctx.TryCancel();
        f.wait();
        return terminal::none;
    };

    SUBCASE("server finishes with OK")
    {
        CHECK(run_call([] { }) == terminal::completed);
    }
    SUBCASE("server finishes with an error status")
    {
        server_status = grpc::Status{grpc::StatusCode::INTERNAL, "server error"};
        CHECK(run_call([] { }) == terminal::error);
    }
    SUBCASE("client cancels")
    {
        server_reads = true;
        CHECK(run_call([&] { ctx.TryCancel(); }) == terminal::error);
    }
    SUBCASE("deadline expires")
    {
        server_reads = true;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds{100});
        CHECK(run_call([] { }) == terminal::error);
    }
    SUBCASE("server shuts down during the call")
    {
        server_reads = true;
        CHECK(run_call([&] { server->Shutdown(std::chrono::system_clock::now()); }) == terminal::error);
    }
    SUBCASE("connection refused, before any read succeeds")
    {
        channel = grpc::CreateChannel("127.0.0.1:1", grpc::InsecureChannelCredentials());
        CHECK(run_call([] { }) == terminal::error);
    }

    server->Shutdown();
    server->Wait();
}
