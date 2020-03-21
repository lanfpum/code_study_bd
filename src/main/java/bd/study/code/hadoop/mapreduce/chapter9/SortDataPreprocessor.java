package bd.study.code.hadoop.mapreduce.chapter9;

import bd.study.code.hadoop.mapreduce.chapter8.JobBuilder;
import bd.study.code.hadoop.mapreduce.maxtemperature.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 *
 * @author 努力常态化
 * @date 2020/3/18 23:05
 */
public class SortDataPreprocessor extends Configured implements Tool {

    static class CleanerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);

            if (parser.isValidTemperature()) {
                context.write(new IntWritable(parser.getAirTemperature()), value);
            }

        }
    }

    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

        if (job == null) {
            return -1;
        }

        job.setMapperClass(CleanerMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortDataPreprocessor(), args);
        System.exit(exitCode);
    }
}
