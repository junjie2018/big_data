package util;

import lombok.Getter;
import org.apache.hadoop.io.Text;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class NcdcRecordParser {
    private static final int MISSING_TEMPERATURE = 9999;

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmm");

    private String observationDateString;

    @Getter
    private String stationId;
    @Getter
    private String year;
    @Getter
    private String airTemperatureString;
    @Getter
    private int airTemperature;
    @Getter
    private boolean airTemperatureMalformed;
    @Getter
    private String quality;

    // xxxxxxxxxxxxxxx1997xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx+10001xxxxxxxx // 测试数据模板
    // xxxxxxxxxxxxxxx1997xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx099992xxxxxxxx // 测试数据模板
    public void parse(String record) {
        stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
        observationDateString = record.substring(15, 27);
        year = record.substring(15, 19);
        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
            airTemperature = Integer.parseInt(airTemperatureString);
        } else if (record.charAt(87) == '-') {
            airTemperatureString = record.substring(87, 92);
            airTemperature = Integer.parseInt(airTemperatureString);
        } else {
            airTemperatureMalformed = true;
        }

        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public static void main(String[] args) {
        NcdcRecordParser parser = new NcdcRecordParser();
        parser.parse("xxxxxxxxxxxxxxx1997xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx+1000axxxxxxxx");
        System.out.println(parser.getYear());
        System.out.println(parser.getAirTemperature());
        System.out.println(parser.getQuality());
    }

    public boolean isValidTemperature() {
        return !airTemperatureMalformed
                && airTemperature != MISSING_TEMPERATURE
                && quality.matches("[01459]");
    }

    public boolean isMalformedTemperature() {
        return airTemperatureMalformed;
    }

    public boolean isMissingTemperature() {
        return airTemperature == MISSING_TEMPERATURE;
    }

    public int getYearInt() {
        return Integer.parseInt(year);
    }

    public Date getObservationDate() {
        try {
            System.out.println(observationDateString);
            return DATE_FORMAT.parse(observationDateString);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
