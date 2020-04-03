package bd.study.code.hadoop.mapreduce.chapter9;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * The world always makes way for the dreamer
 *
 * @author 努力常态化
 * @date 2020/3/23 21:09
 */
public class NcdcStationMetadata {
    private Map<String, String> stationIdAndName = new HashMap<String, String>();

    public void initialize(File file) {
        BufferedReader in = null;

        try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            NcdcStationMetadataParser parser = new NcdcStationMetadataParser();
            String line;

            while ((line = in.readLine()) != null) {
                if (parser.parse(line)) {
                    stationIdAndName.put(parser.getStationId(), parser.getStationName());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }

    public String getStationName(String stationId) {
        String stationName = stationIdAndName.get(stationId);

        if (StringUtils.isBlank(stationName)) {
            return "no available";
        }

        return stationName;
    }
}
