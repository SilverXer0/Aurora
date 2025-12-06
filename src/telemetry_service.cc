#include "telemetry_service.h"
#include "telemetry_types.h"
#include "tracing.h"

#include <chrono>

namespace aurora {

::grpc::Status TelemetryServiceImpl::StreamTelemetry(
    ::grpc::ServerContext* /*context*/,
    ::grpc::ServerReader<aurora::TelemetryFrame>* reader,
    aurora::StreamAck* response)
{

    using ::aurora::tracing::SpanScope;
    SpanScope span("StreamTelemetry", "aurora.telemetry");
    span.set_attr("rpc.method", "StreamTelemetry");

    TelemetryFrame frame;
    std::uint64_t accepted = 0;
    std::uint64_t dropped = 0;

    while (reader->Read(&frame)) {
        TelemetrySample sample;
        sample.satellite_id   = frame.satellite_id();
        sample.timestamp_ms   = frame.timestamp_ms();
        sample.signal_strength = frame.signal_strength();
        sample.temperature_c   = frame.temperature_c();

        if (store_->ingest(std::move(sample))) {
            ++accepted;
        } else {
            ++dropped;
        }
    }

    span.set_attr("ingest.accepted", accepted);
    span.set_attr("ingest.dropped",  dropped);

    response->set_ok(dropped == 0);
    response->set_message("accepted=" + std::to_string(accepted) +
                          " dropped="  + std::to_string(dropped));

    if (dropped > 0) {
        return ::grpc::Status(::grpc::StatusCode::RESOURCE_EXHAUSTED,
                             "Some samples were dropped due to capacity limits");
    }
    return ::grpc::Status::OK;
}

::grpc::Status TelemetryServiceImpl::QueryWindow(
    ::grpc::ServerContext* /*context*/,
    const aurora::QueryRequest* request,
    aurora::QueryResponse* response)
{
    using namespace std::chrono;
    const std::uint32_t window_seconds = request->window_seconds();
    auto window = duration_cast<milliseconds>(
        seconds(window_seconds));

    auto agg = store_->aggregate(window);

    response->set_window_seconds(window_seconds);
    response->set_count(agg.count);

    if (agg.count > 0) {
        response->set_min_signal_strength(agg.min_signal);
        response->set_max_signal_strength(agg.max_signal);
        response->set_min_temperature_c(agg.min_temp);
        response->set_max_temperature_c(agg.max_temp);
        response->set_avg_signal_strength(
            agg.sum_signal / static_cast<double>(agg.count));
        response->set_avg_temperature_c(
            agg.sum_temp / static_cast<double>(agg.count));
    }
    return ::grpc::Status::OK;
}

} // namespace aurora