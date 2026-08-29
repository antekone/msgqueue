#pragma once

#include <cstdlib>
#include <condition_variable>
#include <mutex>
#include <memory>
#include <expected>
#include <optional>
#include <deque>

namespace msgqueue {

class BadState : public std::exception {
public:
};

// --------------------------------------------------------------------

enum class Error : int {
    Ok,                // no error
    BadState,          // some State error, maybe moved out Receiver was used?
    Full,              // can't send, queue is full
    LinkBroken         // a Receiver is without Sender, or Sender without Receiver
};

template <typename A> class Receiver;
template <typename A> class Sender;

namespace internal {

template <typename A>
class State {
public:
    friend class Receiver<A>;
    friend class Sender<A>;

    State& operator= (const State&) = delete;
    State& operator= (State&&) = delete;

private:
    // TODO: in Sender dtor, decrease this
    // TODO: in Sender ctor/clone, increase this
    // TODO: in Receiver dtor, decrease this
    int senders = 0;
    // TODO: in Receiver ctor, set this to true, panic if it's already true
    bool receiverAlive = false;

    std::mutex lock;
    std::deque<A> messages;
    std::condition_variable cv;
};

}

// --------------------------------------------------------------------

// There is just one Receiver.
template <typename A>
class Receiver {
public:
    explicit Receiver(std::shared_ptr<internal::State<A>>&& s) : stateRef(std::move(s)) {
        if (stateRef->receiverAlive) {
            // This shouldn't be possible, unless msgqueue has bad implementation.
            throw BadState {};
        }

        std::unique_lock m(stateRef->lock);
        stateRef->receiverAlive = true;
    }

    Receiver(Receiver&& src) = default;
    Receiver& operator= (Receiver&& src) = default; 

    std::optional<std::expected<A, Error>> tryRecv() {
        if (!stateRef)
            return std::unexpected(Error::BadState);

        std::unique_lock m(stateRef->lock);

        if (stateRef->messages.empty()) {
            // There are no messages in queue.
            return std::nullopt;
        }

        auto&& msg = std::move(stateRef->messages.front());
        stateRef->messages.pop_front();
        return msg;
    }

    std::expected<A, Error> blockingRecv() {
        if (!stateRef)
            return std::unexpected(Error::BadState);

        std::unique_lock m(stateRef->lock);

        return std::unexpected { Error::Ok };
    }

private:
    Receiver(const Receiver&) = delete;
    Receiver& operator= (const Receiver&) = delete;

    std::shared_ptr<internal::State<A>> stateRef;
};

// --------------------------------------------------------------------

// Senders can be cloned.
template <typename A>
class Sender {
public:
    explicit Sender(std::shared_ptr<internal::State<A>> s) : stateRef(s) {
    }

    Error trySend(const A& message) {
        if (!stateRef)
            return Error::BadState;

        std::unique_lock m(stateRef->lock);
        return performSend(message);
    }

    Error trySend(A&& message) {
        if (!stateRef)
            return Error::BadState;

        std::unique_lock m(stateRef->lock);
        return performSend(std::move(message));
    }

private:
    Error performSend(A&& message) {
        stateRef->messages.emplace_back(std::move(message));
        return Error::Ok;
    }

    std::shared_ptr<internal::State<A>> stateRef;
};

// --------------------------------------------------------------------

template <typename A>
class Channel {
public:
    Sender<A> sender;
    Receiver<A> receiver;
};

// --------------------------------------------------------------------

template <typename A>
std::expected<Channel<A>, Error> create(size_t capacity) {
    auto state = std::make_shared<internal::State<A>>();
    return Channel<A> {
        Sender { state },
        Receiver { std::move(state) }
    };
}

// --------------------------------------------------------------------

} // namespace msgqueue
