#pragma once

#include <string>

namespace live::common::security {

// Verifies lowercase/uppercase hex HMAC-SHA256 over "timestamp.raw_body".
// A conventional "sha256=" prefix is accepted.
bool verifyTimestampedHmacSha256(const std::string& secret, const std::string& timestamp,
                                 const std::string& raw_body, std::string signature);

}  // namespace live::common::security
