package com.mplatform.manalytics.mstore.datamatcher.messaging.common;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {

    public static String DEFAULT_FORMAT = "yyyy/MM/dd HH:mm:ss";

    public static String currentDateTime(String format)
    {
        DateFormat dateFormat = new SimpleDateFormat(format);
        Date date = new Date();
        return dateFormat.format(date);
    }
}
