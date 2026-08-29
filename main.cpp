#include <cstdio>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

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
    EXPECT_EQ(mon.hasReceiver(), true);

    {
        auto rx = std::move(chan->receiver);
    }

    EXPECT_EQ(mon.hasReceiver(), false);
}

TEST(msgqueue, disconnectTest) {
    auto chan = msgqueue::create<std::string>(10);

    auto mon = chan->sender.createMonitor(); // keep a monitor instance running

    // additional monitor instance will help us to verify
    // that the monitor itself doesn't keep the channel alive

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
    ASSERT_EQ(msg4.has_value(), true);
    EXPECT_EQ(msg4->error(), msgqueue::Error::Disconnected);
}

TEST(msgqueue, disconnectTest2) {
    auto chan = msgqueue::create<int>(10);
    EXPECT_EQ(chan->sender.trySend(0), msgqueue::Error::Ok);
    { auto r = std::move(chan->receiver); }
    EXPECT_EQ(chan->sender.trySend(1), msgqueue::Error::Disconnected);
}

TEST(msgqueue, monitorTest) {
    auto chan = msgqueue::create<std::string>(10);
    auto mon = chan->sender.createMonitor();
    { auto _ = std::move(chan); }

    EXPECT_EQ(mon.getSenderCount(), 0);
    EXPECT_EQ(mon.hasReceiver(), false);
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

TEST(msgqueue, threadedBlockingSendUnblocksAfterReceive) {
    auto chan = msgqueue::create<std::string>(1);
    ASSERT_TRUE(chan.has_value());
    ASSERT_EQ(chan->sender.trySend("first"), msgqueue::Error::Ok);

    std::promise<msgqueue::Error> sendResult;
    auto sendFuture = sendResult.get_future();
    std::thread sender([&] {
        sendResult.set_value(chan->sender.blockingSend(std::string("second")));
    });

    EXPECT_EQ(sendFuture.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);

    auto first = chan->receiver.blockingRecv();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(*first, "first");

    EXPECT_EQ(sendFuture.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_EQ(sendFuture.get(), msgqueue::Error::Ok);
    sender.join();

    auto second = chan->receiver.blockingRecv();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(*second, "second");
}

TEST(msgqueue, threadedBlockingSendWithClonedSenders) {
    auto chan = msgqueue::create<std::string>(1);
    ASSERT_TRUE(chan.has_value());
    ASSERT_EQ(chan->sender.trySend("first"), msgqueue::Error::Ok);

    auto firstSender = chan->sender;
    auto secondSender = chan->sender;
    std::promise<msgqueue::Error> firstResult;
    std::promise<msgqueue::Error> secondResult;
    auto firstFuture = firstResult.get_future();
    auto secondFuture = secondResult.get_future();

    std::thread firstThread([&] {
        firstResult.set_value(firstSender.blockingSend(std::string("from first sender")));
    });
    std::thread secondThread([&] {
        secondResult.set_value(secondSender.blockingSend(std::string("from second sender")));
    });

    EXPECT_EQ(firstFuture.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    EXPECT_EQ(secondFuture.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);

    auto first = chan->receiver.blockingRecv();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(*first, "first");

    while (firstFuture.wait_for(std::chrono::milliseconds(1)) != std::future_status::ready &&
           secondFuture.wait_for(std::chrono::milliseconds(1)) != std::future_status::ready) {
    }

    auto second = chan->receiver.blockingRecv();
    ASSERT_TRUE(second.has_value());

    EXPECT_EQ(firstFuture.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_EQ(secondFuture.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_EQ(firstFuture.get(), msgqueue::Error::Ok);
    EXPECT_EQ(secondFuture.get(), msgqueue::Error::Ok);
    firstThread.join();
    secondThread.join();

    auto third = chan->receiver.blockingRecv();
    ASSERT_TRUE(third.has_value());
    EXPECT_NE(*second, *third);
    EXPECT_TRUE((*second == "from first sender" && *third == "from second sender") ||
                (*second == "from second sender" && *third == "from first sender"));
}

TEST(msgqueue, threadedBlockingRecv) {
    for (int i = 0; i < 10000; i++) {
        auto chan = msgqueue::create<std::string>(10);
        std::vector<std::string> result;

        auto t = std::thread([&] () {
            while (true) {
                auto msg = chan->receiver.blockingRecv();
                if (!msg.has_value() && msg.error() == msgqueue::Error::Disconnected) {
                    return;
                }

                result.push_back(*msg);
            }
        });

        chan->sender.trySend("hello world");
        chan->sender.trySend("hello world1");
        chan->sender.trySend("hello world2");
        chan->sender.trySend("hello world3");
        chan->destroySender();
        t.join();

        auto shouldBe = std::vector<std::string> { "hello world", "hello world1", "hello world2", "hello world3" };
        ASSERT_EQ(result, shouldBe);
    }
}

TEST(msgqueue, variantTest) {
    enum class Mode { One, Two, Three };
    auto chan = msgqueue::create<std::variant<Mode>>(10);
    chan->sender.blockingSend(Mode::One);
    chan->sender.blockingSend(Mode::Two);
    chan->sender.blockingSend(Mode::Three);
    auto one = chan->receiver.blockingRecv();
    auto two = chan->receiver.blockingRecv();
    auto three = chan->receiver.blockingRecv();
    ASSERT_EQ(one, Mode::One);
    ASSERT_EQ(two, Mode::Two);
    ASSERT_EQ(three, Mode::Three);
}

TEST(msgqueue, objectTest) {
    class Object {
    public:
        Object(const std::string& msg) { something = msg; }
        std::string something;
    };

    auto chan = msgqueue::create<Object>(10);
    chan->sender.blockingSend(Object("abc"));
    chan->sender.blockingSend(std::move(Object("def")));
    auto value = Object("ghi");
    chan->sender.blockingSend(value);
    auto one = chan->receiver.blockingRecv();
    auto two = chan->receiver.blockingRecv();
    auto three = chan->receiver.blockingRecv();
    ASSERT_EQ(one->something, "abc");
    ASSERT_EQ(two->something, "def");
    ASSERT_EQ(three->something, "ghi");
}
