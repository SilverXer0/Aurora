#pragma once

#include "ring_buffer.h"
#include "telemetry_types.h"
#include  "tracing.h"

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include <thread>
#include <memory>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <cstring>

namespace aurora {

class TelemetryShard {
public:
    using Clock = std::chrono::steady_clock;

    explicit TelemetryShard(std::size_t queue_capacity,
                            std::chrono::milliseconds window,
                            const std::string& db_path)
        : queue_(queue_capacity),
          window_(window),
          stop_(false) {
        rocksdb::Options options;
        options.create_if_missing = true;
        options.compression = rocksdb::kSnappyCompression;

        rocksdb::DB* raw = nullptr;
        auto status = rocksdb::DB::Open(options, db_path, &raw);
        if (!status.ok()) {
            throw std::runtime_error(
                "Failed to open RocksDB at " + db_path + ": " +
                status.ToString());
        }
        db_.reset(raw);
        worker_ = std::thread(&TelemetryShard::run, this);
    }
    ~TelemetryShard() {
        stop_.store(true, std::memory_order_relaxed);
        if (worker_.joinable()) worker_.join();
    }

    bool enqueue(TelemetrySample&& s) {
        return queue_.push(std::move(s));
    }

    struct Aggregates {
        std::uint64_t count = 0;
        double min_signal = 0.0, max_signal = 0.0, sum_signal = 0.0;
        double min_temp = 0.0, max_temp = 0.0, sum_temp = 0.0;
    };

    Aggregates snapshot(std::chrono::milliseconds window) const {
        std::lock_guard<std::mutex> lock(mutex_);
        Aggregates agg;
        if (samples_.empty()) return agg;

        auto now = Clock::now();
        auto cutoff = now - window;

        for (const auto& s : samples_) {
            if (s.recv_time < cutoff) continue;
            update_agg(agg, s.signal_strength, s.temperature_c);
        }
        return agg;
    }

public:
    Aggregates aggregate(std::chrono::milliseconds window) const {
        using ::aurora::tracing::SpanScope;
        SpanScope span("TelemetryShard.aggregate", "aurora.telemetry.shard");
        span.set_attr("window_ms", window.count());
        
        auto hot_window = (window <= window_) ? window : window_;
        span.set_attr("hot_window_ms", hot_window.count());

        Aggregates hot = snapshot(hot_window);
        span.set_attr("hot.count", hot.count);

        if (window <= window_) {
            return hot; 
        }

        Aggregates cold = query_cold(window);
        span.set_attr("cold.count", cold.count);

        Aggregates total;
        auto merge_from = [&total](const Aggregates& src) {
            if (src.count == 0) return;
            if (total.count == 0) {
                total.min_signal = src.min_signal;
                total.max_signal = src.max_signal;
                total.min_temp = src.min_temp;
                total.max_temp = src.max_temp;
            } else {
                if (src.min_signal < total.min_signal) total.min_signal = src.min_signal;
                if (src.max_signal > total.max_signal) total.max_signal = src.max_signal;
                if (src.min_temp < total.min_temp) total.min_temp = src.min_temp;
                if (src.max_temp > total.max_temp) total.max_temp = src.max_temp;
            }
            total.sum_signal += src.sum_signal;
            total.sum_temp += src.sum_temp;
            total.count += src.count;
        };

        merge_from(cold);
        merge_from(hot);

        span.set_attr("total.count", total.count);
        return total;
    }

private:
    static void update_agg(Aggregates& agg, double signal, double temp) {
        if (agg.count == 0) {
            agg.min_signal = agg.max_signal = signal;
            agg.min_temp   = agg.max_temp   = temp;
        } else {
            if (signal < agg.min_signal) agg.min_signal = signal;
            if (signal > agg.max_signal) agg.max_signal = signal;
            if (temp   < agg.min_temp)   agg.min_temp   = temp;
            if (temp   > agg.max_temp)   agg.max_temp   = temp;
        }
        agg.sum_signal += signal;
        agg.sum_temp   += temp;
        ++agg.count;
    }

    static std::string make_key(Clock::time_point t) {
        using namespace std::chrono;
        auto ms = duration_cast<milliseconds>(t.time_since_epoch()).count();
        std::uint64_t v = static_cast<std::uint64_t>(ms);

        std::string key(8, '\0');
        for (int i = 7; i >= 0; --i) {
            key[i] = static_cast<char>(v & 0xFF);
            v >>= 8;
        }
        return key;
    }

    static std::string make_value(double signal, double temp) {
        std::string value(sizeof(double) * 2, '\0');
        std::memcpy(&value[0], &signal, sizeof(double));
        std::memcpy(&value[sizeof(double)], &temp, sizeof(double));
        return value;
    }

    Aggregates query_cold(std::chrono::milliseconds total_window) const {
        using ::aurora::tracing::SpanScope;
        SpanScope span("TelemetryShard.query_cold", "aurora.telemetry.shard");
        span.set_attr("window_ms", total_window.count());

        Aggregates agg;

        if (total_window <= window_) return agg;

        using namespace std::chrono;
        auto now = Clock::now();

        auto cold_start = now - total_window; 
        auto cold_end   = now - window_;      

        std::string start_key = make_key(cold_start);
        std::string end_key   = make_key(cold_end);

        rocksdb::ReadOptions ro;
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro));

        for (it->Seek(start_key);
             it->Valid();
             it->Next()) {

            const std::string& k = it->key().ToString();
            if (k >= end_key) break;

            const std::string& v = it->value().ToString();
            if (v.size() < sizeof(double) * 2) continue;

            double signal, temp;
            std::memcpy(&signal, &v[0], sizeof(double));
            std::memcpy(&temp, &v[sizeof(double)], sizeof(double));

            update_agg(agg, signal, temp);
        }

        span.set_attr("cold.count", agg.count);
        return agg;
    }

private:
    struct StoredSample {
        Clock::time_point recv_time;
        double signal_strength;
        double temperature_c;
    };

    void run() {
        using ::aurora::tracing::SpanScope;

        TelemetrySample tmp;
        while (!stop_.load(std::memory_order_relaxed)) {  
            if (!queue_.pop(tmp)) {
                std::this_thread::sleep_for(std::chrono::microseconds(50));
                continue;
            }

            SpanScope span("TelemetryShard.flush_one", "aurora.telemetry.shard");
            span.set_attr("satellite_id", tmp.satellite_id);
            span.set_attr("signal_strength", tmp.signal_strength);
            span.set_attr("temperature_c", tmp.temperature_c);

            auto now = Clock::now();
            {
                std::lock_guard<std::mutex> lock(mutex_);
                samples_.push_back(StoredSample{
                    now, tmp.signal_strength, tmp.temperature_c
                });
                evict_old_locked(now);
            }
            std::string key = make_key(now);
            std::string value = make_value(tmp.signal_strength, tmp.temperature_c);
            auto status = db_->Put(rocksdb::WriteOptions(), key, value);
        }
    }

    void evict_old_locked(Clock::time_point now) {
        auto cutoff = now - window_;
        while (!samples_.empty() && samples_.front().recv_time < cutoff) {
            samples_.pop_front();
        }
    }

    RingBuffer<TelemetrySample> queue_;
    const std::chrono::milliseconds window_;

    mutable std::mutex mutex_;
    std::deque<StoredSample> samples_;

    std::unique_ptr<rocksdb::DB> db_;

    std::atomic<bool> stop_;
    std::thread worker_;
};

} // namespace aurora