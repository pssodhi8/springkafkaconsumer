package com.mplatform.manalytics.mstore.datamatcher.messaging.producer;

import com.rabbitmq.client.BuiltinExchangeType;

import java.util.HashMap;
import java.util.Map;

public class ExchangeProducerConfig
{
    private BuiltinExchangeType exchangeType = BuiltinExchangeType.DIRECT;
    private String exchangeName;
    private String routingKey = "";
    private String queueName;
    private boolean exchange_durable = true;
    private boolean mandatory = true;   // Check if no queue attached to exchange
    private boolean autoAck = false;
    private boolean queue_durable = true;
    private boolean queue_exclusive = false;
    private boolean queue_autoDelete = false;
    private Map<String, Object> arguments = new HashMap<String, Object>();

    public BuiltinExchangeType getExchangeType()
    {
        return exchangeType;
    }

    public ExchangeProducerConfig setExchangeType(BuiltinExchangeType exchangeType)
    {
        this.exchangeType = exchangeType;
        return this;
    }

    public String getExchangeName()
    {
        return exchangeName;
    }

    public ExchangeProducerConfig setExchangeName(String exchangeName)
    {
        this.exchangeName = exchangeName;
        return this;
    }

    public String getRoutingKey()
    {
        return routingKey;
    }

    public ExchangeProducerConfig setRoutingKey(String routingKey)
    {
        this.routingKey = routingKey;
        return this;
    }

    public String getQueueName() { return queueName; }

    public ExchangeProducerConfig setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public boolean isExchange_durable()
    {
        return exchange_durable;
    }

    public ExchangeProducerConfig setExchange_durable(boolean exchange_durable)
    {
        this.exchange_durable = exchange_durable;
        return this;
    }

    public boolean isMandatory()
    {
        return mandatory;
    }

    public ExchangeProducerConfig setMandatory(boolean mandatory)
    {
        this.mandatory = mandatory;
        return this;
    }

    public boolean is_autoAck()
    {
        return autoAck;
    }

    public ExchangeProducerConfig set_autoAck(boolean autoAck)
    {
        this.autoAck = autoAck;
        return this;
    }

    public boolean isQueue_durable()
    {
        return queue_durable;
    }

    public ExchangeProducerConfig setQueue_durable(boolean queue_durable)
    {
        this.queue_durable = queue_durable;
        return this;
    }

    public boolean isQueue_exclusive()
    {
        return queue_exclusive;
    }

    public ExchangeProducerConfig setQueue_exclusive(boolean queue_exclusive)
    {
        this.queue_exclusive = queue_exclusive;
        return this;
    }

    public boolean isQueue_autoDelete()
    {
        return queue_autoDelete;
    }

    public ExchangeProducerConfig setQueue_autoDelete(boolean queue_autoDelete)
    {
        this.queue_autoDelete = queue_autoDelete;
        return this;
    }

    public Map<String, Object> getArguments()
    {
        return arguments;
    }

    public ExchangeProducerConfig setArguments(Map<String, Object> arguments)
    {
        this.arguments = arguments;
        return this;
    }


}
