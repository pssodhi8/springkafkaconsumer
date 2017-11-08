package com.mplatform.manalytics.mstore.datamatcher.messaging.producer;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class QueueProducer implements Producer
{
    private static final Logger logger = LoggerFactory.getLogger(QueueProducer.class);

    private Channel channel;
    private QueueProducerConfig config;
    private Publisher publisher;

    public QueueProducer(Channel channel, QueueProducerConfig config)
    {
        this.channel = channel;
        this.config = config;
    }

     public QueueProducer(Channel channel, QueueProducerConfig config, Publisher publisher)
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
            configureQueue();
            configurePublisher();
        }
        catch (IOException ex)
        {
            logger.error("[QueueProducer.start] Exception: " + Arrays.toString(ex.getStackTrace()));
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
            logger.error("[QueueProducer.configureAck] Exception: " + Arrays.toString(ex.getStackTrace()));
            throw ex;
        }
    }

    private void configureQueue() throws IOException
    {
        try
        {
            channel.queueDeclare
            (
                config.getQueueName(),
                config.isQueue_durable(),
                config.isQueue_exclusive(),
                config.isQueue_autoDelete(),
                config.getArguments()
            );
        }
        catch (IOException ex)
        {
            logger.error("[QueueProducer.configureQueue] Exception: " + Arrays.toString(ex.getStackTrace()));
            throw ex;
        }
    }

    private void configurePublisher()
    {
        if (publisher == null)
            publisher = new QueuePublisher(config);
    }

    @Override
    public boolean sendMessage(String message)
    {
        return publisher.publish(channel, message);
    }

    @Override
    public void close() throws Exception
    {
        channel.close();
    }

}
