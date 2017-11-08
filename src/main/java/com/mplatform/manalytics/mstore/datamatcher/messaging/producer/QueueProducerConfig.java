package com.mplatform.manalytics.mstore.datamatcher.messaging.producer;

import java.util.HashMap;
import java.util.Map;

public class QueueProducerConfig
{
    private String queueName;
    private boolean queue_durable = true;
    private boolean queue_exclusive = false;
    private boolean queue_autoDelete = false;
    private Map<String, Object> arguments = new HashMap<String, Object>();
    private boolean autoAck = false;

    public String getQueueName()
    {
        return queueName;
    }

    public QueueProducerConfig setQueueName(String queueName)
    {
        this.queueName = queueName;
        return this;
    }

    public boolean isQueue_durable()
    {
        return queue_durable;
    }

    public QueueProducerConfig setQueue_durable(boolean queue_durable)
    {
        this.queue_durable = queue_durable;
        return this;
    }

    public boolean isQueue_exclusive()
    {
        return queue_exclusive;
    }

    public QueueProducerConfig setQueue_exclusive(boolean queue_exclusive)
    {
        this.queue_exclusive = queue_exclusive;
        return this;
    }

    public boolean isQueue_autoDelete()
    {
        return queue_autoDelete;
    }

    public QueueProducerConfig setQueue_autoDelete(boolean queue_autoDelete)
    {
        this.queue_autoDelete = queue_autoDelete;
        return this;
    }

    public Map<String, Object> getArguments()
    {
        return arguments;
    }

    public QueueProducerConfig setArguments(Map<String, Object> arguments)
    {
        this.arguments = arguments;
        return this;
    }

    public boolean is_autoAck()
    {
        return autoAck;
    }

    public QueueProducerConfig set_autoAck(boolean autoAck)
    {
        this.autoAck = autoAck;
        return this;
    }
}
