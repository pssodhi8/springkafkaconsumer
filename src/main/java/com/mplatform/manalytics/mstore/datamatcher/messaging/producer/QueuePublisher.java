package com.mplatform.manalytics.mstore.datamatcher.messaging.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Arrays;

public class QueuePublisher implements Publisher
{
    private static final Logger logger = LoggerFactory.getLogger(QueuePublisher.class);

    private QueueProducerConfig config;
    private Exception exception;

    public QueuePublisher(QueueProducerConfig config)
    {
        this.config = config;
    }

    @Override
    public boolean publish(Channel channel, String message)
    {
        try
        {
            channel.basicPublish
            (
                "",     // No exchange name
                config.getQueueName(),
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes("UTF-8")
            );

            if (!config.is_autoAck())
                channel.waitForConfirmsOrDie();

            logger.debug("[QueuePublisher.publish] Sent '"
                    + config.getQueueName()
                    + "', message: '"
                    + message
                    + "'");

            return true;
        }
        catch (IOException|InterruptedException ex)
        {
            logger.error("[QueuePublisher.publish] Exception: " + Arrays.toString(ex.getStackTrace()));
            exception = ex;
            return false;
        }
    }

    public Exception getException()
    {
        return exception;
    }
}
