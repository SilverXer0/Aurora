#pragma once

#include <string>
#include <cstdint>

namespace aurora {

struct TelemetrySample {
    std::string satellite_id;
    std::int64_t timestamp_ms;    
    double signal_strength;
    double temperature_c;
};

} // namespace aurora