package com.mplatform.manalytics.mstore.datamatcher.messaging.producer;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public final class ExchangeProducer implements Producer
{
    private static final Logger logger = LoggerFactory.getLogger(ExchangeProducer.class);

    private ExchangeProducerConfig config;
    private Channel channel;
    private Publisher publisher;

    public ExchangeProducer(Channel channel, ExchangeProducerConfig config)
    {
        this.channel = channel;
        this.config = config;
    }

    public ExchangeProducer(Channel channel, ExchangeProducerConfig config, Publisher publisher)
    {
        this.channel = channel;
        this.config = config;
        this.publisher = publisher;
    }

    @Override
    public void start() throws IOException
    {
        try
        {
            configureAck();
            configureExchange();
            //bypass the queue declare and queue binding if queuename is not set
            if(config.getQueueName() != null && !config.getQueueName().isEmpty() ) {
                configureQueue();
                configureExchangeQueueBinding();
            }
            configurePublisher();
        }
        catch (IOException ex)
        {
            logger.error("[ExchangeProducer.start] Exception: " + Arrays.toString(ex.getStackTrace()));
            throw ex;
        }
    }

    private void configureAck() throws IOException
    {
        try
        {
            if (!config.is_autoAck())
                channel.confirmSelect();    // Enable publisher acknowledgements
        }
        catch (IOException ex)
        {
            logger.error("[ExchangeProducer.configureAck] Exception: " + Arrays.toString(ex.getStackTrace()));
            throw ex;
        }
    }

    private void configureExchange() throws IOException
    {
        try
        {
            // The exchange must already be configured w/ queues or the messages will be lost
            channel.exchangeDeclare
            (
                config.getExchangeName(),
                config.getExchangeType(),
                config.isExchange_durable()
            );

            logger.debug("[ExchangeProducer.configureExchange] Declare exchange: '"
                    + config.getExchangeName()
                    + "', type: '"
                    + config.getExchangeType()
                    + "'");
        }
        catch (IOException ex)
        {
            logger.error("[ExchangeProducer.configureExchange] Exception: " + Arrays.toString(ex.getStackTrace()));
            throw ex;
        }
    }

    private void configureQueue() throws IOException
    {
        try
        {
            channel.queueDeclare(
                    config.getQueueName(),
                    config.isQueue_durable(),
                    config.isQueue_exclusive(),
                    config.isQueue_autoDelete(),
                    config.getArguments()
            );

            logger.debug("[ExchangeProducer.configureQueue] Declare queue: '"
                    + config.getQueueName()
                    + "'");
        }
        catch(IOException ex)
        {
            logger.error("[ExchangeProducer.configureQueue] Exception: " + Arrays.toString(ex.getStackTrace()));
            throw ex;
        }

    }

    private void configureExchangeQueueBinding() throws IOException
    {
        try
        {
            channel.queueBind
            (
                config.getQueueName(),
                config.getExchangeName(),
                config.getRoutingKey()
            );

            logger.debug("[ExchangeProducer.configureExchangeQueueBinding] Bind queue: '"
                    + config.getQueueName()
                    + "', exchange: '"
                    + config.getExchangeName()
                    + "', routing key: '"
                    + config.getRoutingKey()
                    + "'");
        }
        catch(IOException ex)
        {
            logger.error("[ExchangeProducer.configureExchangeQueueBinding] Exception: " + Arrays.toString(ex.getStackTrace()));
            throw ex;
        }
    }

    private void configurePublisher()
    {
        if (publisher == null)
        {
            publisher = new ExchangePublisher(config);
            logger.debug("[ExchangeProducer.configurePublisher] No publisher supplied, creating publisher");
        }
    }

    @Override
    public boolean sendMessage(String message)
    {
        boolean ok = publisher.publish(channel, message);

        logger.debug("[ExchangeProducer.sendMessage] publisher.publish returned: '"
                + String.valueOf(ok)
                + "', message: '"
                + message
                + "'");

        return ok;
    }

    @Override
    public void close() throws Exception
    {
        channel.close();
    }

}
