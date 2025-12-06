#pragma once

#include "telemetry_shard.h"
#include "telemetry_types.h"
#include "tracing.h"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

namespace aurora {

class TelemetryStore {
public:
    using Clock = TelemetryShard::Clock;

    explicit TelemetryStore(std::size_t shard_count,
                            std::size_t queue_capacity,
                            std::chrono::milliseconds window)
    {
        shards_.reserve(shard_count);
        for (std::size_t i = 0; i < shard_count; ++i) {
            std::string db_path = "data/shard_" + std::to_string(i);
            shards_.emplace_back(
                std::make_unique<TelemetryShard>(queue_capacity, window, db_path));
        }
    }

    bool ingest(TelemetrySample&& s) {
        std::size_t h = hash_satellite(s.satellite_id);
        std::size_t idx = h % shards_.size();
        return shards_[idx]->enqueue(std::move(s));
    }

    static std::size_t hash_satellite(const std::string& key) {
        std::size_t h = 1469598103934665603ull;
        for (unsigned char c : key) {
            h ^= c;
            h *= 1099511628211ull;
        }
        return h;
    }

    TelemetryShard::Aggregates aggregate(
        std::chrono::milliseconds window) const
    {

        using ::aurora::tracing::SpanScope;
        SpanScope span("TelemetryStore.aggregate", "aurora.telemetry");
        span.set_attr("window_ms", window.count());
        span.set_attr("shards", shards_.size());


        TelemetryShard::Aggregates total;
        for (const auto& shard : shards_) {
            auto agg = shard->aggregate(window);
            if (agg.count == 0) continue;

            if (total.count == 0) {
                total.min_signal = agg.min_signal;
                total.max_signal = agg.max_signal;
                total.min_temp   = agg.min_temp;
                total.max_temp = agg.max_temp;
            } else {
                if (agg.min_signal < total.min_signal) total.min_signal = agg.min_signal;
                if (agg.max_signal > total.max_signal) total.max_signal = agg.max_signal;
                if (agg.min_temp < total.min_temp) total.min_temp = agg.min_temp;
                if (agg.max_temp > total.max_temp) total.max_temp = agg.max_temp;
            }

            total.sum_signal += agg.sum_signal;
            total.sum_temp += agg.sum_temp;
            total.count += agg.count;
        }
        span.set_attr("total.count", total.count);
        return total;
    }

private:
    static std::size_t shard_index(const std::string& key) {
        std::size_t h = 1469598103934665603ull;
        for (unsigned char c : key) {
            h ^= c;
            h *= 1099511628211ull;
        }
        return h;
    }

    std::vector<std::unique_ptr<TelemetryShard>> shards_;
};

} // namespace aurora