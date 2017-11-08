package com.mplatform.manalytics.mstore.datamatcher.messaging.producer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ReturnListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Arrays;

public class ExchangePublisher implements Publisher
{
    private static final Logger logger = LoggerFactory.getLogger(ExchangePublisher.class);

    private ExchangeProducerConfig config;
    private Exception exception;

    private class MutableBoolean
    {
        private boolean bool = true;
        public MutableBoolean(boolean bool) { this.bool = bool; }
        public boolean getValue() { return this.bool; }
        public void setValue(boolean bool) { this.bool = bool; }
    }

    public ExchangePublisher(ExchangeProducerConfig config)
    {
        this.config = config;
    }

    @Override
    public boolean publish(Channel channel, String message)
    {
        final MutableBoolean status = new MutableBoolean(true);

        try
        {
            if (config.isMandatory())
            {
                // Listener to handle when no queues are assigned to exchange
                channel.addReturnListener(new ReturnListener()
                {
                    @Override
                    public void handleReturn(
                        int replyCode,
                        String replyText,
                        String exchangeName,
                        String routingKey,
                        AMQP.BasicProperties basicProperties,
                        byte[] body) throws IOException
                    {
                        logger.error("[ExchangePublisher.publish] Unroutable message published to exchange: '"
                                + exchangeName
                                + "', replyCode: '"
                                + String.valueOf(replyCode)
                                + "', replyText: '"
                                + replyText
                                + "', routingKey: '"
                                + routingKey
                                + "', message: '"
                                + new String(body, "UTF-8")
                                + "'");

                        status.setValue(false);
                    }
                });
            }

            channel.basicPublish
            (
                config.getExchangeName(),
                config.getRoutingKey(),
                config.isMandatory(),
                new AMQP.BasicProperties.Builder()
                    .contentType("text/plain")
                    .deliveryMode(2)    // 2 = persistent
                    .priority(1)
                    .build(),
                message.getBytes("UTF-8")
            );

            if (!config.is_autoAck())
                channel.waitForConfirmsOrDie();     // check acknowledgment

            logger.debug("[ExchangePublisher.publish] Sending to exchange: '"
                    + config.getExchangeName()
                    + "' w/ routing key: '"
                    + config.getRoutingKey()
                    + "', message: '"
                    + message
                    + "'");
        }
        catch (IOException|InterruptedException ex)
        {
            // IOException - Can mean that basicPublish received a nack
            // InterruptedException - waitForConfirmsOrDie was interrupted
            status.setValue(false);
            exception = ex;
            logger.error("[ExchangePublisher.publish] Exception: '" + Arrays.toString(ex.getStackTrace()) + "'");
        }

        return status.getValue();
    }

    public Exception getException()
    {
        return exception;
    }

}
