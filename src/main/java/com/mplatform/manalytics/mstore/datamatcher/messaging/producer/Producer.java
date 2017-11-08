package com.mplatform.manalytics.mstore.datamatcher.messaging.producer;

import java.io.IOException;

public interface Producer extends AutoCloseable
{
    void start() throws IOException;
    boolean sendMessage(String message);
    void close() throws Exception;
}
