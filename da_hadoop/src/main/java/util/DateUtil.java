package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT);

    public static Date getDate(String dateAsString) {
        try {
            return SIMPLE_DATE_FORMAT.parse(dateAsString);
        } catch (ParseException e) {
            return null;
        }
    }

    public long getDateAsMilliSeconds(String dataAsString) throws Exception {
        Date date = getDate(dataAsString);
        //noinspection ConstantConditions
        return date.getTime();
    }

    public long getDateAsMilliSeconds(Date date){
        return date.getTime();
    }

    public static String getDateAsString(long timestamp) {
        return SIMPLE_DATE_FORMAT.format(timestamp);
    }
}
