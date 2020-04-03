package bd.study.code.hadoop.mapreduce.chapter9;

import bd.study.code.hadoop.mapreduce.chapter8.MinimalMapReduce;
import bd.study.code.hadoop.mapreduce.maxtemperature.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * The world always makes way for the dreamer
 *
 * @author 努力常态化
 * @date 2020/3/23 21:54
 */
public class JoinRecordWithStationName extends Configured implements Tool {
    static class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text> {
        private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (parser.parse(value)) {
                context.write(new TextPair(parser.getStationId(), "0"), new Text(parser.getStationName()));
            }
        }
    }

    static class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            context.write(new TextPair(parser.getStationId(), "1"), value);
        }
    }

    static class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
        @Override
        protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            Text stationName = new Text(iter.next());

            while (iter.hasNext()) {
                Text record = iter.next();
                Text outValue = new Text(stationName.toString() + "\t" + record.toString());
                // getFirst 为气象站id
                context.write(key.getFirst(), outValue);
            }
        }
    }

    static class KeyPartitioner extends Partitioner<TextPair, Text> {

        @Override
        public int getPartition(TextPair key, Text value, int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            MinimalMapReduce.printUsage(this, "<ncdc input> <station input> <output>");
            return -1;
        }

        Job job = new Job(getConf(), "Join weather records with station names");
        job.setJarByClass(getClass());

        Path ncdcInputPath = new Path(args[0]);
        Path stationInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        MultipleInputs.addInputPath(job, ncdcInputPath, TextInputFormat.class, JoinRecordMapper.class);
        MultipleInputs.addInputPath(job, stationInputPath, TextInputFormat.class, JoinStationMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new JoinRecordWithStationName(), args);
        System.exit(exitCode);
    }
}
