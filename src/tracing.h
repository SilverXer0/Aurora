#pragma once

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

namespace aurora::tracing {

class Span {
public:
    Span(std::string name, std::string component = "")
        : name_(std::move(name)),
          component_(std::move(component)),
          start_(Clock::now())
    {
        std::cerr << "[span:start] name=" << name_;
        if (!component_.empty()) {
            std::cerr << " component=" << component_;
        }
        std::cerr << " thread=" << std::this_thread::get_id() << "\n";
    }

    ~Span() {
        auto end = Clock::now();
        auto dur = std::chrono::duration_cast<std::chrono::microseconds>(end - start_).count();
        std::cerr << "[span:end]   name=" << name_
                  << " duration_us=" << dur
                  << " thread=" << std::this_thread::get_id()
                  << "\n";
    }

    template <typename T>
    void set_attr(const std::string& key, const T& value) {
        // Simple attribute logging for now
        std::cerr << "[span:attr]  name=" << name_
                  << " " << key << "=" << value << "\n";
    }

private:
    using Clock = std::chrono::steady_clock;
    std::string name_;
    std::string component_;
    Clock::time_point start_;
};

class SpanScope {
public:
    SpanScope(const std::string& name,
              const std::string& component = "")
        : span_(name, component) {}

    template <typename T>
    void set_attr(const std::string& key, const T& value) {
        span_.set_attr(key, value);
    }

private:
    Span span_;
};

#define AURORA_TRACE_SCOPE(name) \
    ::aurora::tracing::SpanScope AURORA_SPAN_##__LINE__{name, "aurora"}

} // namespace aurora::tracing