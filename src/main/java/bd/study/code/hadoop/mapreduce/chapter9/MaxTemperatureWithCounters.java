package bd.study.code.hadoop.mapreduce.chapter9;

import bd.study.code.hadoop.mapreduce.chapter8.JobBuilder;
import bd.study.code.hadoop.mapreduce.maxtemperature.MaxTemperatureReducer;
import bd.study.code.hadoop.mapreduce.maxtemperature.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 *
 * @author 努力常态化
 * @date 2020/3/18 22:21
 */
public class MaxTemperatureWithCounters extends Configured implements Tool {

    enum Temperature {
        // 缺失
        MISSING,
        // 畸形
        MALFORMED
    }

    static class MaxTemperatureMapperWithCounters extends Mapper<LongWritable, Text, Text, IntWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);

            if (parser.isValidTemperature()) {
                int airTemperature = parser.getAirTemperature();
                context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
            } else if (parser.isMalformedTemperature()) {
                System.err.println("Ignorring possibly corrupt input :" + value);
                context.getCounter(Temperature.MALFORMED).increment(1);
            } else if (parser.isMissingTemperature()) {
                context.getCounter(Temperature.MISSING).increment(1);
            }

            context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
        }
    }

    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        job.setMapperClass(MaxTemperatureMapperWithCounters.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
        System.exit(exitCode);
    }
}
