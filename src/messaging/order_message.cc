#include "messaging/order_message.h"

#include <limits>

namespace live::messaging {
namespace {

constexpr std::uint32_t kMagic = 0x464f5131;  // FOQ1

void appendU16(std::string* out, std::uint16_t value) {
    out->push_back(static_cast<char>((value >> 8) & 0xff));
    out->push_back(static_cast<char>(value & 0xff));
}

void appendU32(std::string* out, std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) out->push_back(static_cast<char>((value >> shift) & 0xff));
}

bool readU16(const std::string& value, std::size_t* offset, std::uint16_t* result) {
    if (offset == nullptr || result == nullptr || *offset + 2 > value.size()) return false;
    *result = static_cast<std::uint16_t>((static_cast<unsigned char>(value[*offset]) << 8) |
                                         static_cast<unsigned char>(value[*offset + 1]));
    *offset += 2;
    return true;
}

bool readU32(const std::string& value, std::size_t* offset, std::uint32_t* result) {
    if (offset == nullptr || result == nullptr || *offset + 4 > value.size()) return false;
    *result = 0;
    for (int i = 0; i < 4; ++i) *result = (*result << 8) | static_cast<unsigned char>(value[*offset + i]);
    *offset += 4;
    return true;
}

void appendField(std::string* out, const std::string& field) {
    appendU32(out, static_cast<std::uint32_t>(field.size()));
    out->append(field);
}

bool readField(const std::string& value, std::size_t* offset, std::string* field) {
    std::uint32_t length = 0;
    if (!readU32(value, offset, &length) || length > 4096 || *offset + length > value.size()) return false;
    field->assign(value.data() + *offset, length);
    *offset += length;
    return true;
}

}  // namespace

common::Status FlashSaleOrderMessage::validate() const {
    if (version != 1 || promotion_id.empty() || order_id.empty() || idempotency_key.empty() ||
        user_id.empty() || sku_id.empty() || quantity <= 0 ||
        promotion_id.size() > 64 || order_id.size() > 64 || idempotency_key.size() > 128 ||
        user_id.size() > 64 || sku_id.size() > 64 ||
        quantity > static_cast<std::int64_t>(std::numeric_limits<std::uint32_t>::max())) {
        return common::Status::InvalidArgument("invalid flash-sale order message");
    }
    return common::Status::Ok();
}

std::string FlashSaleOrderMessage::serialize() const {
    if (!validate().ok()) return {};
    std::string value;
    appendU32(&value, kMagic);
    appendU16(&value, version);
    appendField(&value, promotion_id);
    appendField(&value, order_id);
    appendField(&value, idempotency_key);
    appendField(&value, user_id);
    appendField(&value, sku_id);
    appendU32(&value, static_cast<std::uint32_t>(quantity));
    return value;
}

common::Status FlashSaleOrderMessage::deserialize(const std::string& value, FlashSaleOrderMessage* message) {
    if (message == nullptr || value.size() < 6 || value.size() > 32 * 1024) {
        return common::Status::InvalidArgument("invalid flash-sale order payload");
    }
    std::size_t offset = 0;
    std::uint32_t magic = 0;
    if (!readU32(value, &offset, &magic) || magic != kMagic || !readU16(value, &offset, &message->version) ||
        !readField(value, &offset, &message->promotion_id) || !readField(value, &offset, &message->order_id) ||
        !readField(value, &offset, &message->idempotency_key) || !readField(value, &offset, &message->user_id) ||
        !readField(value, &offset, &message->sku_id)) {
        return common::Status::InvalidArgument("malformed flash-sale order payload");
    }
    std::uint32_t quantity = 0;
    if (!readU32(value, &offset, &quantity) || offset != value.size()) {
        return common::Status::InvalidArgument("malformed flash-sale order quantity");
    }
    message->quantity = quantity;
    return message->validate();
}

}  // namespace live::messaging
