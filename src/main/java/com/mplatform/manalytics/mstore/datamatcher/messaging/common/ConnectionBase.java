package com.mplatform.manalytics.mstore.datamatcher.messaging.common;

import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface ConnectionBase extends AutoCloseable
{
    void connect() throws IOException, TimeoutException, InterruptedException;
    Channel createChannel() throws IOException, InterruptedException;
    void destroy() throws Exception;

    @Override
    void close() throws Exception;
}
