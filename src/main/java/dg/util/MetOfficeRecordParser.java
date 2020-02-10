package dg.util;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

public class MetOfficeRecordParser implements Serializable {
    @Getter
    private String year;
    @Getter
    private String airTemperatureString;
    @Getter
    private int airTemperature;

    private boolean airTemperatureValid;

    public void parse(String record) {
        if (record.length() < 18) {
            return;
        }
        year = record.substring(3, 7);
        if (isValidRecord(year)) {
            airTemperatureString = record.substring(13, 18);
            if (!airTemperatureString.trim().equals("---")) {
                BigDecimal temp = new BigDecimal(airTemperatureString.trim());
                temp = temp.multiply(new BigDecimal(BigInteger.TEN));
                airTemperature = temp.intValueExact();
                airTemperatureValid = true;
            }
        }
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    private boolean isValidRecord(String year) {
        if (StringUtils.isNotEmpty(year)) {
            return year.trim().matches("[0-9]+");
        }
        return false;
    }

    public boolean isValidTemperature() {
        return airTemperatureValid;
    }
}
