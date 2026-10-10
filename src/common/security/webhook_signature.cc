#include "common/security/webhook_signature.h"

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

#include <array>
#include <limits>

namespace live::common::security {

bool verifyTimestampedHmacSha256(const std::string& secret, const std::string& timestamp,
                                 const std::string& raw_body, std::string signature) {
    if (secret.empty() || secret.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) return false;
    if (signature.rfind("sha256=", 0) == 0) signature.erase(0, 7);
    if (signature.size() != 64) return false;
    const std::string message = timestamp + "." + raw_body;
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int digest_length = 0;
    if (HMAC(EVP_sha256(), secret.data(), static_cast<int>(secret.size()),
             reinterpret_cast<const unsigned char*>(message.data()), message.size(), digest.data(), &digest_length) == nullptr ||
        digest_length != 32) return false;
    static constexpr char hex[] = "0123456789abcdef";
    std::array<char, 64> expected{};
    for (std::size_t i = 0; i < 32; ++i) {
        expected[i * 2] = hex[digest[i] >> 4];
        expected[i * 2 + 1] = hex[digest[i] & 0x0f];
    }
    for (char& c : signature) if (c >= 'A' && c <= 'F') c = static_cast<char>(c - 'A' + 'a');
    return CRYPTO_memcmp(signature.data(), expected.data(), expected.size()) == 0;
}

}  // namespace live::common::security
