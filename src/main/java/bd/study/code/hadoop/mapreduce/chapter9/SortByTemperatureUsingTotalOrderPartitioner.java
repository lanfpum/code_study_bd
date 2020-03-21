package bd.study.code.hadoop.mapreduce.chapter9;

import bd.study.code.hadoop.mapreduce.chapter8.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * The world always makes way for the dreamer
 * 全局排序
 *
 * @author 努力常态化
 * @date 2020/3/19 21:24
 */
public class SortByTemperatureUsingTotalOrderPartitioner extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

        if (job == null) {
            return -1;
        }

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        job.setPartitionerClass(TotalOrderPartitioner.class);
        InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);
        InputSampler.writePartitionFile(job, sampler);
        Configuration configuration = job.getConfiguration();
        String partitionFile = TotalOrderPartitioner.getPartitionFile(configuration);
        URI partitionUri = new URI(partitionFile);
        job.addCacheFile(partitionUri);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{
                "D:\\workDir\\otherFile\\temp\\airtemperature_data\\testoutput",
                "D:\\workDir\\otherFile\\temp\\airtemperature_data\\testoutput2"
        };

        int exitCode = ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitioner(), args);
        System.exit(exitCode);
    }
}
