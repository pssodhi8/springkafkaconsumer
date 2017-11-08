package com.mplatform.manalytics.mstore.datamatcher;

import com.mplatform.manalytics.mstore.datamatcher.messaging.common.ConnectionBase;
import com.mplatform.manalytics.mstore.datamatcher.messaging.common.RabbitConnection;
import com.mplatform.manalytics.mstore.datamatcher.messaging.common.RetryConnection;
import com.mplatform.manalytics.mstore.datamatcher.messaging.producer.*;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerMain {
    private static final Logger logger = LoggerFactory.getLogger(ProducerMain.class);
    private static int port = 5672;
    private static String exchange = "mstore-exchange-2";
    private static String queue = "mstore-queue-2";
    private static String routingKey = "";
    
    private static String host = "localhost";
    private static String virtualHost = "/";
    private static String userName = "guest";
    private static String password = "guest";

    public static void SendFanoutExchange() throws Exception
    {
        ExchangeProducerConfig config =
            new ExchangeProducerConfig()
                .setExchangeType(BuiltinExchangeType.FANOUT)
                .setExchangeName(exchange);

        Publisher publisher = new ExchangePublisher(config);
        Publisher retryPublisher = new RetryPublisher(3, 500, publisher);

        try (ConnectionBase retryConnection =
                 new RetryConnection(
                         2,
                         1000,
                         new RabbitConnection(host,port,userName,password,virtualHost)))
        {
            retryConnection.connect();

            Producer producer = new ExchangeProducer(retryConnection.createChannel(), config, retryPublisher);
            producer.start();

            System.out.println(" [x] Type a message to send and Enter; type 'exit' to terminate...");
            Scanner in = new Scanner(System.in);
            while (true)
            {
                String line = in.nextLine();
                if (line.startsWith("exit"))
                    break;

                boolean ok = producer.sendMessage(line);
                System.out.println(" [x] ExchangeProducer.sendMessage returned '" + (ok ? "true" : "false") + "'");
            }
        }
    }

    public static void SendRetryExchange(String message) throws Exception
    {
        ExchangeProducerConfig config =
            new ExchangeProducerConfig()
                .setExchangeType(BuiltinExchangeType.TOPIC)
                .setExchangeName("TopicExchange")
                .setQueueName("TopicQueue")
                .setRoutingKey("topic.key");

        Publisher publisher =
            new RetryPublisher(10, 1000, 2, new ExchangePublisher(config));

        try (ConnectionBase retryConnection =
                new RetryConnection(
                        10,
                        1000,
                        2,
                        new RabbitConnection(host,port,userName,password,virtualHost)))
        {
            retryConnection.connect();

            try (Producer producer = new ExchangeProducer(retryConnection.createChannel(), config, publisher))
            {
                producer.start();
                boolean ok = producer.sendMessage(message);
                logger.info(" [x] ExchangeProducer.sendMessage returned '" + (ok ? "true" : "false") + "'");
            }
        }
    }

    public static void SendTopicExchange() throws Exception
    {
        ExchangeProducerConfig config =
            new ExchangeProducerConfig()
                .setExchangeType(BuiltinExchangeType.TOPIC)
                .setExchangeName(exchange)
                .setRoutingKey(routingKey)
                .setQueueName(queue);


        Publisher publisher =
            new RetryPublisher(10, 1000, 2, new ExchangePublisher(config));

        try (ConnectionBase retryConnection =
                new RetryConnection(
                        10,
                        1000,
                        2,
                        new RabbitConnection(host,port,userName,password,virtualHost)))
        {
            retryConnection.connect();

            try (Producer producer = new ExchangeProducer(retryConnection.createChannel(), config, publisher))
            {
                producer.start();

                System.out.println(" [x] Type a message to send and Enter; type 'exit' to terminate...");
                Scanner in = new Scanner(System.in);
                while (true)
                {
                    String line = in.nextLine();
                    if (line.startsWith("exit"))
                        break;

                    boolean ok = producer.sendMessage(line);
                    System.out.println(" [x] ExchangeProducer.sendMessage returned '" + (ok ? "true" : "false") + "'");
                }
            }
        }
    }

    public static void SendQueue() throws Exception
    {
        QueueProducerConfig config = new QueueProducerConfig();
        config.setQueueName(queue);

        Publisher publisher = new QueuePublisher(config);
        Publisher retryPublisher = new RetryPublisher(3, 500, publisher);

        RabbitConnection connection = new RabbitConnection(host,port,userName,password,virtualHost);
        connection.connect();

        Channel channel = connection.createChannel();
        Producer queue = new QueueProducer(channel, config, retryPublisher);
        queue.start();

        System.out.println(" [x] Enter a message to send. Type 'exit' to terminate...");
        Scanner in = new Scanner(System.in);
        while (true)
        {
            String line = in.nextLine();
            if (line.startsWith("exit"))
                break;

            boolean ok = queue.sendMessage(line);
            System.out.println(" [x] QueueProducer.sendMessage returned '" + (ok ? "true" : "false") + "'");
        }

        connection.close();
    }

}
