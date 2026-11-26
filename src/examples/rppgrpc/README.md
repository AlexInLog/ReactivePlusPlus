# Writing to a client stream after `init()`

Use `rppgrpc::client_status_hooks` when an external producer sends requests after the client-streaming RPC starts. The hooks install a gRPC client interceptor that closes the writer and releases its gRPC hold when the RPC ends, even if the producer is idle.

Install the interceptor once when creating the channel. You can add your own interceptor factories to the same vector. The interceptor chain must forward `POST_RECV_STATUS` so the writer can release its hold.

```cpp
rppgrpc::client_status_hooks hooks;
auto factories = hooks.make_interceptors();
auto channel = grpc::experimental::CreateCustomChannelWithInterceptors(
    target, credentials, channel_args, std::move(factories));
auto stub = TestService::NewStub(channel);
```

Use a fresh `ClientContext` for each call. Pass the same context to the reactor and to the generated stub, and keep the context and response alive until the observable terminates.

```cpp
grpc::ClientContext context;
Response response;
auto* reactor = new rppgrpc::client_write_reactor<Request>{context, hooks};
stub->async()->ClientSide(&context, &response, reactor);

auto writer = reactor->get_observer();
reactor->get_observable().subscribe([](rpp::utils::none) {}, on_error, on_completed);
reactor->init();

// From the producer.
writer.on_next(request);
writer.on_completed();
```

Obtain the observer and subscribe before `init()`. The reactor deletes itself when the call ends. The observer drops values still queued at that point and any later emissions. A missing interceptor cancels the call and reports an error through the observable. The default reactor constructor remains available for callers that submit requests before `init()`.

The [runnable example](doxygen/client_write_status.cpp) starts two calls on one stub. The server rejects an idle writer after sending headers, while the other writer can still send a request after `init()`. The example also checks the error from using a plain channel. With gRPC examples enabled, build and run `client_write_status_grpc_doxygen_sample`.
