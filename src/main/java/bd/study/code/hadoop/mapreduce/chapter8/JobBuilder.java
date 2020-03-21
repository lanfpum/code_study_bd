package bd.study.code.hadoop.mapreduce.chapter8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2020/3/18 20:50.
 */
public class JobBuilder {
    public static Job parseInputAndOutput(Tool tool, Configuration conf, String[] args) throws IOException {
        return MinimalMapReduce.parseInputAndOutput(tool, conf, args);
    }

    public static void exitJVM(Tool tool, String[] args) {
        int exitCode = 0;
        try {
            exitCode = ToolRunner.run(tool, args);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
