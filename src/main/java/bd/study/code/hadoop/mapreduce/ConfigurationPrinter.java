package bd.study.code.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * The world always makes way for the dreamer
 *
 * @author 努力常态化
 * @date 2020/3/1 15:56
 */
public class ConfigurationPrinter extends Configured implements Tool {

    static {
//        Configuration.addDefaultResource("hdfs-site.xml");
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        for (Map.Entry<String, String> entry : conf) {
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
        System.exit(exitCode);
    }
}
