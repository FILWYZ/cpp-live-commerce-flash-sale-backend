#pragma once

#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <cstdio>
#include <unistd.h>
#include <signal.h>

namespace live::common::testing {

// A one-shot process crash hook for isolated integration tests. It is inert
// unless both the explicit opt-in and a per-run marker path are configured.
inline void crashOnceAt(const char* point) {
    const char* enabled = std::getenv("ECOMMERCE_ENABLE_TEST_FAULTS");
    const char* configured_point = std::getenv("ECOMMERCE_TEST_CRASH_POINT");
    const char* marker_path = std::getenv("ECOMMERCE_TEST_CRASH_MARKER");
    if (point == nullptr || enabled == nullptr || std::strcmp(enabled, "1") != 0 ||
        configured_point == nullptr || std::strcmp(configured_point, point) != 0 ||
        marker_path == nullptr || *marker_path == '\0') {
        return;
    }
    const int marker = ::open(marker_path, O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (marker < 0) return;  // The same container has already taken this crash.
    (void)::close(marker);
    std::fprintf(stderr, "test fault injection: crashing at %s\n", point);
    (void)::kill(::getpid(), SIGKILL);
    std::_Exit(86);  // Fallback if the platform refuses to deliver SIGKILL.
}

}  // namespace live::common::testing
