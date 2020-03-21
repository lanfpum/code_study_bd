package bd.study.code.hadoop.mapreduce.chapter8;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 *
 * @author 努力常态化
 * @date 2020/3/10 20:12
 */
public class MinimalMapReduce extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("Usage:%s [generic options] <input> <output> \n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job(getConf());
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"D:\\workDir\\otherFile\\temp\\2018-12-4", "D:\\workDir\\otherFile\\temp\\2018-12-4\\output"};
        System.exit(ToolRunner.run(new MinimalMapReduce(), args));
    }

    public static Job parseInputAndOutput(Tool tool, Configuration conf, String[] args) throws IOException {
        if (args.length < 2 || StringUtils.isBlank(args[0])) {
            args = new String[]{
                    "D:\\workDir\\otherFile\\temp\\airtemperature_data\\data4test",
                    "D:\\workDir\\otherFile\\temp\\airtemperature_data\\testoutput"
            };
        }

        if (args.length != 2) {
            printUsage(tool, "<input> <output>");
            return null;
        }

        Job job = new Job(conf);
        job.setJarByClass(tool.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job;
    }

    public static void printUsage(Tool tool, String extraArgsUsage) {
        System.err.printf("Usage:%s [generic options] %s \n\n", tool.getClass().getSimpleName(), extraArgsUsage);
        GenericOptionsParser.printGenericCommandUsage(System.err);
    }

}
