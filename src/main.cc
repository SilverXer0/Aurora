#include "telemetry_store.h"
#include "telemetry_service.h"

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>

int main(int argc, char** argv) {
    (void)argc; (void)argv;

    const std::size_t shard_count = 8;
    const std::size_t queue_capacity = 1 << 16;
    const auto window_ms = std::chrono::milliseconds(60'000); 

    auto store = std::make_shared<aurora::TelemetryStore>(
        shard_count, queue_capacity, window_ms);

    std::string server_address("0.0.0.0:50051");
    aurora::TelemetryServiceImpl service(store);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address,
                             grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Aurora server listening on " << server_address << "\n";

    server->Wait();
    return 0;
}