#include <cstdio>

#include "msgqueue/queue.h"

int main() {
    try {
        auto chan = msgqueue::create<std::string>(10);
        if (!chan) {
            printf("channel creation error\n");
            return 1;
        }

        auto ret = chan->sender.trySend("hello world");
        chan->sender.trySend("hello world2");
        if (ret != msgqueue::Error::Ok) {
            printf("err\n");
            return 1;
        }

        for (int i = 0; i < 3; i++) {
            auto maybeS = chan->receiver.tryRecv();
            if (!maybeS) {
                printf("no message\n");
            } else {
                if (maybeS->has_value()) {
                    printf("value: %s\n", maybeS->value().c_str());
                } else {
                    printf("error: 0x%08x\n", maybeS->error());
                }
            }
        }

        return 0;
    } catch (const std::exception& e) {
        printf("exception: %s\n", e.what());
        return 1;
    }
}
