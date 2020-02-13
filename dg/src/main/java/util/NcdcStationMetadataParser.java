package util;

import lombok.Getter;
import org.apache.hadoop.io.Text;

public class NcdcStationMetadataParser {
    @Getter
    private String stationId;
    @Getter
    private String stationName;

    public boolean parse(String record) {
        if (record.length() < 42) {
            return false;
        }
        String usaf = record.substring(0, 6);
        String wban = record.substring(7, 12);
        stationId = usaf + "-" + wban;
        stationName = record.substring(13, 42);

        try {
            Integer.parseInt(usaf);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean parse(Text record) {
        return parse(record.toString());
    }
}
