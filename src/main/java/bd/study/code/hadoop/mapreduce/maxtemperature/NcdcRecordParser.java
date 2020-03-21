package bd.study.code.hadoop.mapreduce.maxtemperature;

import org.apache.hadoop.io.Text;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * The world always makes way for the dreamer
 *
 * @author 努力常态化
 * @date 2020/3/1 20:51
 */
public class NcdcRecordParser {
    private static final int        MISSING_TEMPERATURE = 9999;
    private static final DateFormat DATE_FORMAT         = new SimpleDateFormat("yyyyMMddHHmm");

    private String stationId;
    private String observationDateString;
    private String airTemperatureString;

    private String year;
    private int    airTemperature;
    private String quality;

    private boolean airTemperatureMalformed;

    public void parse(String record) {
        stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
        observationDateString = record.substring(15, 27);
        year = record.substring(15, 19);
        airTemperatureMalformed = false;

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

    public boolean isValidTemperature() {
        return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }

    public boolean isMalformedTemperature() {
        return airTemperatureMalformed;
    }

    public boolean isMissingTemperature() {
        return airTemperature == MISSING_TEMPERATURE;
    }

    public String getStationId() {
        return stationId;
    }

    public String getYear() {
        return year;
    }

    public int getYearInt() {
        return Integer.parseInt(year);
    }

    public int getAirTemperature() {
        return airTemperature;
    }

    public String getAirTemperatureString() {
        return airTemperatureString;
    }

    public String getQuality() {
        return quality;
    }
}
