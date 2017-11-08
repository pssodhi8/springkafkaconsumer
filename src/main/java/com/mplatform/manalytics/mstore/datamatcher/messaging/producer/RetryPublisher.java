package com.mplatform.manalytics.mstore.datamatcher.messaging.producer;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;

public class RetryPublisher implements Publisher
{
    public static final int RETRY_INFINITE = -1;
    public final static int BACKOFF_CEILING_INFINITE = -1;

    private static final Logger logger = LoggerFactory.getLogger(RetryPublisher.class);

    private int retries = 2;
    private long sleep = 1000;              // milliseconds
    private int backoffMultiplier = 1;      // exponential backoff multiplier
    private int backoffCeiling = 2;         // max # of backoff attempts
    private Publisher publisher;

    public RetryPublisher(int retries, long sleep, Publisher publisher)
    {
        this.retries = retries;
        this.sleep = sleep;
        this.publisher = publisher;
    }

    public RetryPublisher(int retries, long sleep, int backoffMultiplier, Publisher publisher)
    {
        this(retries, sleep, publisher);
        this.backoffMultiplier = backoffMultiplier;
    }

    public RetryPublisher(int retries, long sleep, int backoffMultiplier, int backoffCeiling, Publisher publisher)
    {
        this(retries, sleep, backoffMultiplier, publisher);
        this.backoffCeiling = backoffCeiling;
    }

    @Override
    public boolean publish(Channel channel, String message)
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
            if (publisher.publish(channel, message))
                return true;

            if (i == localRetries)
            {
                logger.error("[RetryPublisher.publish] Retry attempts '" + String.valueOf(retries) + "' exceeded.");
                return false;
            }

            if (retries == RETRY_INFINITE)
                logger.error("[RetryPublisher.publish] failed, retrying.");
            else
                logger.error("[RetryPublisher.publish] failed, retry attempt '" + String.valueOf(i + 1) + "'");

            try
            {
                Thread.sleep(localSleep);
            }
            catch (InterruptedException ex)
            {
                logger.error("[RetryPublisher.publish] sleep interrupted exception: " + Arrays.toString(ex.getStackTrace()));
            }

            if (backoffCeiling == BACKOFF_CEILING_INFINITE || localBackoffCeiling < backoffCeiling)
                localSleep *= backoffMultiplier;
        }

        return false;
    }
}
