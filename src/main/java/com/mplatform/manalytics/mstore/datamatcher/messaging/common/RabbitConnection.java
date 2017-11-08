package com.mplatform.manalytics.mstore.datamatcher.messaging.common;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class RabbitConnection implements ConnectionBase
{
    private static final Logger logger = LoggerFactory.getLogger(RabbitConnection.class);

    private List<Address> addresses = new ArrayList<>();

    private String userName;
    private String password;
    private String virtualHost = "/";
    private ConnectionFactory factory;
    private Connection connection;

    public RabbitConnection(String host)
    {
        this.addresses.add(new Address(host, 5672));
    }

    public RabbitConnection(String host, int port, String userName, String password, String virtualHost)
    {
        this.addresses.add(new Address(host, port));
        this.userName = userName;
        this.password = password;
        this.virtualHost = virtualHost;
    }

    public RabbitConnection(String host, int port, String userName, String password)
    {
        this.addresses.add(new Address(host, port));
        this.userName = userName;
        this.password = password;
    }

    public RabbitConnection(Address[] addresses, String userName, String password, String virtualHost)
    {
        logger.warn("WARNING: Multiple connection addresses provided. This should only be used in a clustered environment.");

        for (Address address: addresses)
            this.addresses.add(address);

        this.userName = userName;
        this.password = password;
        this.virtualHost = virtualHost;
    }

    @Override
    public void connect() throws IOException, TimeoutException
    {
        configureFactory();
        configureConnection();
    }

    private void configureFactory()
    {
        try
        {
            factory = new ConnectionFactory();

            if (addresses.size() == 1)
            {
                factory.setHost(addresses.get(0).getHost());
                factory.setPort(addresses.get(0).getPort());
            }

            if (userName != null && !userName.isEmpty())
                factory.setUsername(userName);

            if (password != null && !password.isEmpty())
                factory.setPassword(password);

            factory.setVirtualHost(virtualHost);
            //factory.setAutomaticRecoveryEnabled(true);
            //factory.setTopologyRecoveryEnabled(true);

            factory.setExceptionHandler(new ExceptionHandler() {
                @Override
                public void handleUnexpectedConnectionDriverException(Connection connection, Throwable throwable)
                {
                    logger.error("handleUnexpectedConnectionDriverException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");

                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) +
                        ") handleUnexpectedConnectionDriverException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");
                }

                @Override
                public void handleReturnListenerException(Channel channel, Throwable throwable)
                {
                    logger.error("handleReturnListenerException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");

                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) +
                        ") handleReturnListenerException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");
                }

                @Override
                public void handleFlowListenerException(Channel channel, Throwable throwable)
                {
                    logger.error("handleFlowListenerException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");

                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) +
                        ") handleFlowListenerException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");
                }

                @Override
                public void handleConfirmListenerException(Channel channel, Throwable throwable)
                {
                    logger.error("handleConfirmListenerException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");

                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) +
                        ") handleConfirmListenerException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");
                }

                @Override
                public void handleBlockedListenerException(Connection connection, Throwable throwable)
                {
                    logger.error("handleBlockedListenerException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");

                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) +
                        ") handleBlockedListenerException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");
                }

                @Override
                public void handleConsumerException(Channel channel, Throwable throwable, Consumer consumer, String s, String s1)
                {
                    logger.error("handleConsumerException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Consumer '" + consumer.toString() + "' " +
                        "s '" + s + "' " +
                        "s1 '" + s1 + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");

                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) +
                        ") handleConsumerException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Consumer '" + consumer.toString() + "' " +
                        "s '" + s + "' " +
                        "s1 '" + s1 + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");
                }

                @Override
                public void handleConnectionRecoveryException(Connection connection, Throwable throwable)
                {
                    logger.error("handleConnectionRecoveryException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");

                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) +
                        ") handleConnectionRecoveryException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");
                }

                @Override
                public void handleChannelRecoveryException(Channel channel, Throwable throwable)
                {
                    logger.error("handleChannelRecoveryException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");

                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) +
                        ") handleChannelRecoveryException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(throwable.getStackTrace()) + "'");
                }

                @Override
                public void handleTopologyRecoveryException(Connection connection, Channel channel, TopologyRecoveryException e)
                {
                    logger.error("handleTopologyRecoveryException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(e.getStackTrace()) + "'");

                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) +
                        ") handleTopologyRecoveryException for " +
                        "ID '" + connection.getId() + "' " +
                        "Host '" + connection.getAddress().getHostName() + "' " +
                        "Port '" + connection.getPort() + "' " +
                        "Exception '" + Arrays.toString(e.getStackTrace()) + "'");
                }
            });
        }
        catch (Exception ex)
        {
            logger.error("RabbitConnection.configureFactory() exception: " + Arrays.toString(ex.getStackTrace()));
            throw ex;
        }
    }

    private void configureConnection()
        throws TimeoutException, IOException
    {
        try
        {
            if (addresses.size() == 1)
                connection = factory.newConnection();
            else
                connection = factory.newConnection(addresses);
        }
        catch (TimeoutException | IOException ex)
        {
            logger.error("RabbitConnection.configureConnection() exception: " + Arrays.toString(ex.getStackTrace()));
            throw ex;
        }
    }

    @Override
    public Channel createChannel() throws IOException
    {
        try
        {
            return connection.createChannel();
        }
        catch (IOException ex)
        {
            logger.error("RabbitConnection.createChannel() exception: " + Arrays.toString(ex.getStackTrace()));
            throw ex;

        }

    }

    public void destroy() throws Exception
    {
        try
        {
            if (connection != null)
            {
                connection.close();
            }
        }
        catch (Exception ex)
        {
            logger.error("RabbitConnection.destroy() exception: " + Arrays.toString(ex.getStackTrace()));
            throw ex;
        }
    }

    @Override
    public void close() throws Exception
    {
        destroy();
    }

    public Address[] getAddresses()
    {
        return addresses.stream().toArray(Address[]::new);
    }

    public ConnectionFactory getFactory() {
        return factory;
    }

    public Connection getConnection() {
        return connection;
    }

}
