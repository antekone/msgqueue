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

namespace internal {

template <typename A>
class State {
public:
    friend class Receiver<A>;
    friend class Sender<A>;
    friend class StateMonitor<A>;

    State(size_t c) : capacity(c) { }

    State& operator= (const State&) = delete;
    State& operator= (State&&) = delete;

private:
    // TODO: in Receiver dtor, decrease this
    int senders = 0;
    // TODO: in Receiver ctor, set this to true, panic if it's already true
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
    StateMonitor(const std::shared_ptr<internal::State<A>>& s) : stateRef(s) {}

    int getSenderCount() const { return stateRef->senders; }
    bool hasReceiver() const { return stateRef->receiverAlive; }

private:
    std::shared_ptr<internal::State<A>> stateRef;
};

// --------------------------------------------------------------------

// There is just one Receiver.
template <typename A>
class Receiver {
public:
    friend class Channel<A>;

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

    StateMonitor<A> createMonitor() { return StateMonitor { stateRef }; }

    std::optional<std::expected<A, Error>> tryRecv() {
        if (!stateRef)
            return std::unexpected(Error::BadState);

        std::unique_lock m(stateRef->lock);

        if (!stateRef->messages.empty()) {
            auto&& msg = std::move(stateRef->messages.front());
            stateRef->messages.pop_front();
            return msg;
        }

        // If we don't have any messages in queue, check if there any any
        // senders left. If there are none, this means we'll never get a
        // message on this queue again, so we flag it as Disconnected, so
        // parent can quit.
        
        if (stateRef->senders == 0)
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
            bool hasSenders = stateRef->senders > 0;
            return hasMessages || !hasSenders;
        });

        if (!stateRef->messages.empty()) {
            A msg = std::move(stateRef->messages.front());
            stateRef->messages.pop_front();
            return msg;
        }

        // If we don't have any messages in queue, check if there any any
        // senders left. If there are none, this means we'll never get a
        // message on this queue again, so we flag it as Disconnected, so
        // parent can quit.
        
        if (stateRef->senders == 0)
            return std::unexpected { Error::Disconnected };

        return std::unexpected { Error::Internal };
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
    friend class Channel<A>;

    explicit Sender(std::shared_ptr<internal::State<A>> s) : stateRef(s) {
        if (stateRef) {
            std::unique_lock m(stateRef->lock);
            stateRef->senders++;
        }
    }

    Sender(const Sender<A>& s) : stateRef(s.stateRef) {
        if (stateRef) {
            std::unique_lock m(stateRef->lock);
            stateRef->senders++;
        }
    }

    Sender(Sender<A>&& s) : stateRef(std::move(s.stateRef)) {
    }

    Sender<A>& operator= (const Sender<A>& s) {
        if (this == &s) 
            return *this;

        if (stateRef) {
            std::unique_lock m1(stateRef->lock);
            stateRef->senders--;
            notifyIfNeeded();
            m1.unlock();
        }

        stateRef = s.stateRef;

        if (stateRef) {
            std::unique_lock m(stateRef->lock);
            stateRef->senders++;
        }

        return *this;
    }

    Sender<A>& operator= (Sender<A>&& s) {
        if (this == &s) 
            return *this;

        if (stateRef) {
            std::unique_lock m1(stateRef->lock);
            stateRef->senders--;
            notifyIfNeeded();
            m1.unlock();
        }

        stateRef = std::move(s.stateRef);
        return *this;
    }

    ~Sender() {
        if (stateRef) {
            std::unique_lock m(stateRef->lock);
            stateRef->senders--;
            notifyIfNeeded();
        }
    }

    StateMonitor<A> createMonitor() { return StateMonitor { stateRef }; }

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

    Error blockingSend(const A& message) {
        // TODO
        return Error::Internal;
    }

    Error blockingSend(A&& message) {
        // TODO
        return Error::Internal;
    }

private:
    Error performSend(A&& message) {
        if (stateRef->messages.size() >= stateRef->capacity) {
            return Error::Full;
        }

        stateRef->messages.emplace_back(std::move(message));
        return Error::Ok;
    }

    void notifyIfNeeded() {
        if (stateRef) {
            if (stateRef->senders == 0) {
                stateRef->receiveRead.notify_all();
            }
        }
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
    if (capacity < 1) {
        return std::unexpected { Error::BadState };
    }

    auto state = std::make_shared<internal::State<A>>(capacity);
    return Channel<A> {
        Sender { state },
        Receiver { std::move(state) }
    };
}

// --------------------------------------------------------------------

} // namespace msgqueue
