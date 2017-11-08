package com.mplatform.manalytics.mstore.datamatcher.messaging.producer;

import com.rabbitmq.client.Channel;

public interface Publisher
{
    boolean publish(Channel channel, String message);

}
