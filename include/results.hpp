#pragma once
#include <cstdio>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <string>

#include "latency_histogram.hpp"

// Shells out to `git rev-parse` so each result row is tied to the commit
// that produced it — this is what makes results/history.csv a real
// changelog of the project's latency over time, not just a log file.
inline std::string git_short_hash() {
    FILE* pipe = popen("git rev-parse --short HEAD 2>/dev/null", "r");
    if (!pipe) return "unknown";
    char buf[64] = {0};
    bool ok = fgets(buf, sizeof(buf), pipe) != nullptr;
    pclose(pipe);
    if (!ok) return "unknown";
    std::string hash(buf);
    while (!hash.empty() && (hash.back() == '\n' || hash.back() == '\r')) hash.pop_back();
    return hash.empty() ? "unknown" : hash;
}

// Appends one row per run to a CSV. See scripts/plot_latency.py for
// turning this into a chart.
inline void append_result_csv(const std::string& path, const std::string& run_label,
                               uint64_t event_count, const LatencyHistogram& hist) {
    std::filesystem::path p(path);
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());

    bool exists = std::filesystem::exists(p);
    std::ofstream out(path, std::ios::app);
    if (!exists) {
        out << "timestamp,git_hash,run_label,n,min_ns,mean_ns,p50_ns,p90_ns,p99_ns,p99_9_ns,"
               "p99_99_ns,max_ns\n";
    }

    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    char ts[32];
    std::strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%S", std::localtime(&t));

    out << ts << ',' << git_short_hash() << ',' << run_label << ',' << event_count << ','
        << hist.min() << ',' << hist.mean() << ',' << hist.percentile(50) << ','
        << hist.percentile(90) << ',' << hist.percentile(99) << ',' << hist.percentile(99.9)
        << ',' << hist.percentile(99.99) << ',' << hist.max() << '\n';
}