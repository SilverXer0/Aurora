#pragma once

#include "telemetry_store.h"
#include "telemetry.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <memory>

namespace aurora {

class TelemetryServiceImpl final : public aurora::TelemetryService::Service {
public:
    explicit TelemetryServiceImpl(std::shared_ptr<TelemetryStore> store)
        : store_(std::move(store)) {}

    ::grpc::Status StreamTelemetry(
        ::grpc::ServerContext* context,
        ::grpc::ServerReader<aurora::TelemetryFrame>* reader,
        aurora::StreamAck* response) override;

    ::grpc::Status QueryWindow(
        ::grpc::ServerContext* context,
        const aurora::QueryRequest* request,
        aurora::QueryResponse* response) override;

private:
    std::shared_ptr<TelemetryStore> store_;
};

} // namespace aurora