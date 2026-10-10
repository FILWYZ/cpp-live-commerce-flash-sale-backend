#include "business/user/user_service.h"
#include "messaging/async_outbox_dispatcher.h"
#include "messaging/in_memory_broker.h"
#include "messaging/outbox.h"
#include "kv/kv_store.h"

#include <chrono>
#include <cstdio>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

void testRandomTokenAndLogout() {
    live::business::user::UserService users(nullptr, std::chrono::seconds(3));
    live::business::user::User user;
    check(users.registerUser("security-user", "secret", live::business::user::UserRole::kCustomer, &user).ok(), "user register");
    live::business::user::Session first;
    live::business::user::Session second;
    check(users.login("security-user", "secret", &first).ok(), "first login");
    check(users.login("security-user", "secret", &second).ok(), "second login");
    check(first.token.size() == 64 && second.token.size() == 64 && first.token != second.token,
          "session tokens must be random and non-repeating");
    live::business::user::User authenticated;
    check(users.authenticate(first.token, &authenticated).ok(), "session authenticate");
    check(users.logout(first.token).ok(), "session logout");
    check(users.authenticate(first.token, &authenticated).code() == live::common::ErrorCode::kUnauthenticated,
          "logged out session remained valid");
    check(users.authenticate(second.token, &authenticated).ok(), "second session should remain valid before TTL");
    std::this_thread::sleep_for(std::chrono::milliseconds(3100));
    check(users.authenticate(second.token, &authenticated).code() == live::common::ErrorCode::kUnauthenticated,
          "expired session remained valid");
}

void testSharedSessionStore() {
    auto store = std::make_shared<live::kv::InMemoryKVStore>();
    live::business::user::UserService users_a(nullptr, store.get());
    live::business::user::UserService users_b(nullptr, store.get());
    live::business::user::User user;
    check(users_a.registerUser("shared-session-user", "secret", live::business::user::UserRole::kCustomer, &user).ok(), "shared session register");
    live::business::user::Session session;
    check(users_a.login("shared-session-user", "secret", &session).ok(), "shared session login");
    // Simulate a second API instance with the same shared session and user
    // store, but without a local user database.
    check(users_b.restore().ok(), "shared session restore");
    live::business::user::User authenticated;
    check(users_b.authenticate(session.token, &authenticated).ok() && authenticated.username == "shared-session-user",
          "shared session was not accepted by the second instance");
}

void testDurableOutboxRecoveryAndRetry() {
    const std::string file = "/tmp/live-commerce-outbox-test.bin";
    std::remove(file.c_str());
    {
        live::messaging::FileOutbox outbox;
        check(outbox.open(file).ok(), "outbox open");
        check(outbox.append({"event-1", "OrderCreated", "order-1", "payload", 1}).ok(), "outbox append");
    }
    live::messaging::FileOutbox recovered;
    check(recovered.open(file).ok() && recovered.size() == 1, "outbox recovery");
    live::messaging::InMemoryBroker broker;
    int attempts = 0;
    auto publish = [&broker, &attempts](const live::messaging::Event& event) {
        if (++attempts == 1) return live::common::Status::Internal("temporary broker failure");
        return broker.publish(event);
    };
    check(!recovered.drain(publish).ok() && recovered.size() == 1, "outbox retained failed delivery");
    check(recovered.drain(publish).ok() && recovered.size() == 0, "outbox retry delivery");
    check(broker.pending("OrderCreated") == 1, "outbox event was not delivered");
    std::remove(file.c_str());

    const std::string corrupt_file = "/tmp/live-commerce-outbox-corrupt.bin";
    std::remove(corrupt_file.c_str());
    {
        live::messaging::FileOutbox outbox;
        check(outbox.open(corrupt_file).ok(), "corrupt outbox open");
        check(outbox.append({"event-2", "OrderPaid", "order-2", "payload", 2}).ok(), "corrupt outbox append");
    }
    {
        std::fstream stream(corrupt_file, std::ios::in | std::ios::out | std::ios::binary);
        check(stream.good(), "corrupt outbox mutate open");
        stream.seekp(-1, std::ios::end);
        char byte = 0;
        stream.read(&byte, 1);
        stream.seekp(-1, std::ios::end);
        byte ^= static_cast<char>(0x55);
        stream.write(&byte, 1);
    }
    live::messaging::FileOutbox corrupted;
    check(corrupted.open(corrupt_file).code() == live::common::ErrorCode::kInternal,
          "outbox checksum must reject corruption");
    std::remove(corrupt_file.c_str());
}

}  // namespace

int main() {
    try {
        testRandomTokenAndLogout();
        testSharedSessionStore();
        testDurableOutboxRecoveryAndRetry();
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "security_outbox_test failed: %s\n", error.what());
        return 1;
    }
}
