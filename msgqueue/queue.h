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
    Internal,          // programmer error, shouldn't happen
    Disconnected       // a Receiver is without Sender, or Sender without Receiver
};

template <typename A> class Receiver;
template <typename A> class Sender;
template <typename A> class Channel;
template <typename A> class StateMonitor;
template <typename A> class SenderLifetime;

namespace internal {

template <typename A>
class State {
public:
    friend class Receiver<A>;
    friend class Sender<A>;
    friend class StateMonitor<A>;
    friend class SenderLifetime<A>;

    State(size_t c) : capacity(c) { }

    State& operator= (const State&) = delete;
    State& operator= (State&&) = delete;

private:
    bool receiverAlive = false;
    size_t capacity = 0;
    std::mutex lock;
    std::deque<A> messages;
    std::condition_variable notFull;
    // blockingRead should be woken up
    std::condition_variable receiveRead;
};

}

template <typename A>
class StateMonitor {
public:
    StateMonitor(
        const std::weak_ptr<SenderLifetime<A>>& l,
        const std::weak_ptr<internal::State<A>>& stateRef
    ) : weakLifetime(l), weakState(stateRef) {}

    int getSenderCount() const { 
        return weakLifetime.use_count();
    }

    bool hasReceiver() const { 
        auto p = weakState.lock();
        if (p) {
            return p->receiverAlive; 
        } else {
            // if State doesn't exist, this means there are no Senders nor
            // Receivers left.
            return false;
        }
    }

private:
    std::weak_ptr<SenderLifetime<A>> weakLifetime;
    std::weak_ptr<internal::State<A>> weakState;
};

// --------------------------------------------------------------------

// There is just one Receiver.
template <typename A>
class Receiver {
public:
    friend class Channel<A>;

    explicit Receiver(
        std::shared_ptr<internal::State<A>>&& s,
        std::weak_ptr<SenderLifetime<A>>& lifetime
    ) : stateRef(std::move(s)), weakSenderLifetime(lifetime) {
        if (stateRef->receiverAlive) {
            // This shouldn't be possible, unless msgqueue has bad implementation.
            throw BadState {};
        }

        std::unique_lock m(stateRef->lock);
        stateRef->receiverAlive = true;
    }

    ~Receiver() {
        if (stateRef) {
            std::unique_lock m(stateRef->lock);
            stateRef->receiverAlive = false;
            stateRef->notFull.notify_all();
        }
    }

    Receiver(Receiver&& src) = default;
    Receiver& operator= (Receiver&& src) = default; 

    std::optional<std::expected<A, Error>> tryRecv() {
        if (!stateRef)
            return std::unexpected(Error::BadState);

        std::unique_lock m(stateRef->lock);

        if (!stateRef->messages.empty()) {
            auto&& msg = std::move(stateRef->messages.front());
            stateRef->messages.pop_front();
            stateRef->notFull.notify_all();
            return msg;
        }

        // If we don't have any messages in queue, check if there any any
        // senders left. If there are none, this means we'll never get a
        // message on this queue again, so we flag it as Disconnected, so
        // parent can quit.
        
        if (weakSenderLifetime.expired())
            return std::unexpected { Error::Disconnected };

        // We have senders, but queue is empty.
        return std::nullopt;
    }

    std::expected<A, Error> blockingRecv() {
        if (!stateRef)
            return std::unexpected(Error::BadState);

        std::unique_lock m(stateRef->lock);

        stateRef->receiveRead.wait(m, [&] () {
            bool hasMessages = !stateRef->messages.empty();
            bool hasSenders = !weakSenderLifetime.expired();
            return hasMessages || !hasSenders;
        });

        if (!stateRef->messages.empty()) {
            A msg = std::move(stateRef->messages.front());
            stateRef->messages.pop_front();
            stateRef->notFull.notify_all();
            return msg;
        }

        // If we don't have any messages in queue, check if there any any
        // senders left. If there are none, this means we'll never get a
        // message on this queue again, so we flag it as Disconnected, so
        // parent can quit.
        
        if (weakSenderLifetime.expired())
            return std::unexpected { Error::Disconnected };

        return std::unexpected { Error::Internal };
    }

private:
    Receiver(const Receiver&) = delete;
    Receiver& operator= (const Receiver&) = delete;

    std::shared_ptr<internal::State<A>> stateRef;
    std::weak_ptr<SenderLifetime<A>> weakSenderLifetime;
};

// --------------------------------------------------------------------

template <typename A>
class SenderLifetime {
public:
    friend class Sender<A>;
    friend class StateMonitor<A>;

    SenderLifetime(std::shared_ptr<internal::State<A>> s) : stateRef(s) { }
    ~SenderLifetime() {
        if (stateRef) {
            std::unique_lock _(stateRef->lock);
            stateRef->receiveRead.notify_all();
        }
    }

private:
    std::shared_ptr<internal::State<A>> stateRef;
};

// Senders can be cloned.
template <typename A>
class Sender {
public:
    friend class Channel<A>;

    explicit Sender(std::shared_ptr<SenderLifetime<A>> s) : lifetime(s) {
    }

    StateMonitor<A> createMonitor() { 
        return StateMonitor { 
            std::weak_ptr<SenderLifetime<A>>(lifetime) ,
            std::weak_ptr<internal::State<A>>(lifetime->stateRef),
        }; 
    }

    Error trySend(const A& message) {
        if (!lifetime || !lifetime->stateRef)
            return Error::BadState;

        std::unique_lock m(lifetime->stateRef->lock);
        return performSend(message);
    }

    Error trySend(A&& message) {
        if (!lifetime || !lifetime->stateRef)
            return Error::BadState;

        std::unique_lock m(lifetime->stateRef->lock);
        return performSend(std::move(message));
    }

    Error blockingSend(const A& message) {
        if (!lifetime || !lifetime->stateRef)
            return Error::BadState;

        std::unique_lock m(lifetime->stateRef->lock);
        return performBlockingSend(m, message);
    }

    Error blockingSend(A&& message) {
        if (!lifetime || !lifetime->stateRef)
            return Error::BadState;

        std::unique_lock m(lifetime->stateRef->lock);
        return performBlockingSend(m, std::move(message));
    }

private:
    Error sanityCheck() {
        if (!lifetime->stateRef->receiverAlive)
            return Error::Disconnected;

        if (lifetime->stateRef->messages.size() >= lifetime->stateRef->capacity) {
            return Error::Full;
        }

        return Error::Ok;
    }

    Error performSend(A&& message) {
        auto err = sanityCheck();
        if (err != Error::Ok)
            return err;

        lifetime->stateRef->messages.emplace_back(std::move(message));
        lifetime->stateRef->receiveRead.notify_all();
        return Error::Ok;
    }

    Error performBlockingSend(std::unique_lock<std::mutex>& lock, A&& message) {
        auto err = sanityCheck();
        if (err == Error::Full) {
            lifetime->stateRef->notFull.wait(lock, [&] {
                auto notFull = lifetime->stateRef->messages.size() < lifetime->stateRef->capacity;
                auto hasReceivers = lifetime->stateRef->receiverAlive;
                return notFull || !hasReceivers;
            });
        } else if (err != Error::Ok) {
            return err;
        }

        return performSend(std::move(message));
    }

    std::shared_ptr<SenderLifetime<A>> lifetime;
};

// --------------------------------------------------------------------

template <typename A>
class Channel {
public:
    Sender<A> sender;
    Receiver<A> receiver;

    void destroySender() {
        auto _ = std::move(sender);
    }

    void destroyListener() {
        auto _ = std::move(receiver);
    }
};

// --------------------------------------------------------------------

template <typename A>
std::expected<Channel<A>, Error> create(size_t capacity) {
    if (capacity < 1) {
        return std::unexpected { Error::BadState };
    }

    auto state = std::make_shared<internal::State<A>>(capacity);
    auto lifetime = std::make_shared<SenderLifetime<A>>(state);
    std::weak_ptr<SenderLifetime<A>> weakLifetime = lifetime;

    return Channel<A> {
        Sender<A> { lifetime },
        Receiver<A> { std::move(state), weakLifetime }
    };
}

// --------------------------------------------------------------------

} // namespace msgqueue
