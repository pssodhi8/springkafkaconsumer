package com.mplatform.manalytics.mstore.datamatcher.messaging.common;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class RetryConnection implements ConnectionBase
{
    public final static int RETRY_INFINITE = -1;
    public final static int BACKOFF_CEILING_INFINITE = -1;

    private static final Logger logger = LoggerFactory.getLogger(RetryConnection.class);

    private int retries = 2;
    private long sleep = 1000;              // milliseconds
    private int backoffMultiplier = 1;      // exponential backoff multiplier
    private int backoffCeiling = 2;         // max # of backoff attempts
    private ConnectionBase connection;

    public RetryConnection(int retries, long sleep, ConnectionBase connection)
    {
        this.retries = retries;
        this.sleep = sleep;
        this.connection = connection;
    }

    public RetryConnection(int retries, long sleep, int backoffMultiplier, ConnectionBase connection)
    {
        this(retries, sleep, connection);
        this.backoffMultiplier = backoffMultiplier;
    }

    public RetryConnection(int retries, long sleep, int backoffMultiplier, int backoffCeiling, ConnectionBase connection)
    {
        this(retries, sleep, backoffMultiplier, connection);
        this.backoffCeiling = backoffCeiling;
    }

    @Override
    public void connect() throws IOException, TimeoutException, InterruptedException
    {
        long localSleep = sleep;
        int localRetries = retries;
        int increment = 1;

        if (retries == RETRY_INFINITE)
        {
            localRetries = 1;
            increment = 0;
        }

        for (int i = 0, localBackoffCeiling = 0; i <= localRetries; i += increment, localBackoffCeiling++)
        {
            try
            {
                connection.connect();
                return;
            }
            catch (IOException|TimeoutException|InterruptedException ex)
            {
                if (i == localRetries)
                {
                    logger.error("RetryConnection.connect() retry attempts of '" + String.valueOf(retries) + "' exceeded. Exception: '" + Arrays.toString(ex.getStackTrace()) + "'");
                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) + ") RetryConnection.connect() retry attempts of '" + String.valueOf(retries) + "' exceeded. Exception: '" + Arrays.toString(ex.getStackTrace()) + "'");
                    throw ex;
                }

                if (retries == RETRY_INFINITE)
                {
                    logger.error("RetryConnection.connect() failed, retrying. Exception: '" + Arrays.toString(ex.getStackTrace()) + "'");
                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) + ") RetryConnection.connect() failed, retrying. Exception: '" + Arrays.toString(ex.getStackTrace()) + "'");
                }
                else
                {
                    logger.error("RetryConnection.connect() failed, retry attempt '" + String.valueOf(i+1) + "'. Exception: '" + Arrays.toString(ex.getStackTrace()) + "'");
                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) + ") RetryConnection.connect() failed, retry attempt '" + String.valueOf(i+1) + "'. Exception: '" + Arrays.toString(ex.getStackTrace()) + "'");
                }

                try
                {
                    Thread.sleep(localSleep);
                }
                catch (InterruptedException itex)
                {
                    logger.debug("RetryConnection.connect() sleep interrupted exception: " + Arrays.toString(itex.getStackTrace()));
                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) + ") RetryConnection.connect() sleep interrupted exception: " + Arrays.toString(itex.getStackTrace()));
                }

                if (backoffCeiling == BACKOFF_CEILING_INFINITE || localBackoffCeiling < backoffCeiling)
                    localSleep *= backoffMultiplier;
            }
        }
    }

    @Override
    public Channel createChannel() throws IOException, InterruptedException
    {
        long localSleep = sleep;
        int localRetries = retries;
        int increment = 1;

        if (retries == RETRY_INFINITE)
        {
            localRetries = 1;
            increment = 0;
        }

        for (int i = 0, localBackoffCeiling = 0; i <= localRetries; i += increment, localBackoffCeiling++)
        {
            try
            {
                return connection.createChannel();
            }
            catch (IOException|InterruptedException ex)
            {
                if (i == localRetries)
                {
                    logger.error("RetryConnection.createChannel() retry attempts of '" + String.valueOf(retries) + "' exceeded. Exception: '" + Arrays.toString(ex.getStackTrace()) + "'");
                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) + ") RetryConnection.createChannel() retry attempts of '" + String.valueOf(retries) + "' exceeded. Exception: '" + Arrays.toString(ex.getStackTrace()) + "'");
                    throw ex;
                }

                if (retries == RETRY_INFINITE)
                {
                    logger.error("RetryConnection.createChannel() failed, retrying. Exception: '" + Arrays.toString(ex.getStackTrace()) + "'");
                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) + ") RetryConnection.createChannel() failed, retrying. Exception: '" + Arrays.toString(ex.getStackTrace()) + "'");
                }
                else
                {
                    logger.error("RetryConnection.createChannel() failed, retry attempt '" + String.valueOf(i + 1) + "'. Exception: '" + Arrays.toString(ex.getStackTrace()) + "'");
                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) + ") RetryConnection.createChannel() failed, retry attempt '" + String.valueOf(i + 1) + "'. Exception: '" + Arrays.toString(ex.getStackTrace()) + "'");
                }

                try
                {
                    Thread.sleep(localSleep);
                }
                catch (InterruptedException itex)
                {
                    logger.debug("RetryConnection.createChannel() sleep interrupted exception: " + Arrays.toString(itex.getStackTrace()));
                    System.out.println(" [x] (" + DateUtil.currentDateTime(DateUtil.DEFAULT_FORMAT) + ") RetryConnection.createChannel() sleep interrupted exception: " + Arrays.toString(itex.getStackTrace()));
                }

                if (backoffCeiling == BACKOFF_CEILING_INFINITE || localBackoffCeiling < backoffCeiling)
                    localSleep *= backoffMultiplier;
            }
        }

        return null;
    }

    @Override
    public void destroy() throws Exception
    {
        connection.destroy();
    }

    @Override
    public void close() throws Exception
    {
        destroy();
    }

}
