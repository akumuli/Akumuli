#include <iostream>
#include "tcp_server.h"

int main(int argc, char** argv) {
    Akumuli::Logger logger("test", 10);
    logger.info() << "Info";
    for (int i = 0; i < 10; i++)
        logger.trace() << "Trace msg " << i;
    logger.error() << "Hello " << "world";
    return 0;
}

