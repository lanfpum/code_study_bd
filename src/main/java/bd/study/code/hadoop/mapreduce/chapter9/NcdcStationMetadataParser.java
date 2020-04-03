package bd.study.code.hadoop.mapreduce.chapter9;


import org.apache.hadoop.io.Text;

/**
 * The world always makes way for the dreamer
 *
 * @author 努力常态化
 * @date 2020/3/23 21:13
 */
public class NcdcStationMetadataParser {
    private String stationId;
    private String stationName;

    public boolean parse(String record) {
        String usaf = record.substring(0, 6);
        String wban = record.substring(7, 12);
        stationId = usaf + "-" + wban;
        stationName = record.substring(13, 42);
        //去除前后空格
        stationName = stationName.trim();
        //去除无效数据
        if (stationName.equals("")) {
            return false;
        } else {
            try {
                //usaf是纯数字标识，转换不过来就说明是有问题
                Integer.parseInt(usaf);
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    public boolean parse(Text text) {
        return parse(text.toString());
    }

    public String getStationId() {
        return stationId;
    }

    public String getStationName() {
        return stationName;
    }
}
