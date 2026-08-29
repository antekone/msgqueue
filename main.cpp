#include <cstdio>

#include "gtest/gtest.h"
#include "msgqueue/queue.h"

TEST(msgqueue, create) {
    auto chan = msgqueue::create<std::string>(10);
    EXPECT_TRUE(chan.has_value());
}

TEST(msgqueue, senderCount) {
    auto chan = msgqueue::create<std::string>(10);
    auto mon = chan->sender.createMonitor();

    EXPECT_EQ(mon.getSenderCount(), 1);
    EXPECT_EQ(mon.hasReceiver(), true);

    {
        auto tx2 = chan->sender;
        EXPECT_EQ(mon.getSenderCount(), 2);
    }

    {
        auto tx = std::move(chan->sender);
    }

    EXPECT_EQ(mon.getSenderCount(), 0);
}

TEST(msgqueue, disconnectTest) {
    auto chan = msgqueue::create<std::string>(10);
    auto mon = chan->sender.createMonitor();

    EXPECT_EQ(chan->sender.trySend("first"), msgqueue::Error::Ok);
    EXPECT_EQ(chan->sender.trySend("second"), msgqueue::Error::Ok);
    EXPECT_EQ(chan->sender.trySend("third"), msgqueue::Error::Ok);

    auto msg1 = chan->receiver.tryRecv();
    EXPECT_EQ(msg1->value(), "first");

    { auto s = std::move(chan->sender); }

    // Queued messages should still be received
    
    auto msg2 = chan->receiver.tryRecv();
    EXPECT_EQ(msg2->value(), "second");
    auto msg3 = chan->receiver.tryRecv();
    EXPECT_EQ(msg3->value(), "third");
    auto msg4 = chan->receiver.tryRecv();
    EXPECT_EQ(msg4->error(), msgqueue::Error::Disconnected);
}

TEST(msgqueue, create0) {
    auto chan = msgqueue::create<std::string>(0);
    EXPECT_FALSE(chan.has_value());
}

TEST(msgqueue, send) {
    auto chan = msgqueue::create<std::string>(10);
    auto ret = chan->sender.trySend("hello world");
    EXPECT_EQ(ret, msgqueue::Error::Ok);
}

TEST(msgqueue, sendAboveCapacity) {
    auto chan = msgqueue::create<std::string>(2);

    auto ret = chan->sender.trySend("hello world");
    EXPECT_EQ(ret, msgqueue::Error::Ok);

    auto ret2 = chan->sender.trySend("hello world");
    EXPECT_EQ(ret2, msgqueue::Error::Ok);

    auto ret3 = chan->sender.trySend("hello world");
    EXPECT_EQ(ret3, msgqueue::Error::Full);
}

