package bd.study.code.hadoop.mapreduce.chapter9;

import bd.study.code.hadoop.mapreduce.chapter8.JobBuilder;
import bd.study.code.hadoop.mapreduce.maxtemperature.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * 二次排序：对值进行排序
 *
 * @author 努力常态化
 * @date 2020/3/21 20:48
 */
public class MaxTemperatureUsingSecondarySort extends Configured implements Tool {

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);

            if (parser.isValidTemperature()) {
                context.write(new IntPair(parser.getYearInt(), parser.getAirTemperature()), NullWritable.get());
            }
        }
    }

    static class MaxTemperatureReducer extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {
        @Override
        protected void reduce(IntPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    /**
     * 分区方式：按年份分区
     */
    public static class FirstPartitioner extends Partitioner<IntPair, NullWritable> {
        @Override
        public int getPartition(IntPair key, NullWritable value, int numPartitions) {
            // multiply by 127 to perform some mixing 乘以127进行混合
            return Math.abs(key.getYear() * 127) % numPartitions;
        }
    }

    /**
     * 键比较方式
     */
    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair pair1 = (IntPair) a;
            IntPair pair2 = (IntPair) b;
            return pair1.compareTo(pair2);
        }
    }

    /**
     * 分组方式
     */
    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair pair1 = (IntPair) a;
            IntPair pair2 = (IntPair) b;
            return pair1.getYear() - pair2.getYear();
        }
    }

    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

        if (job == null) {
            return -1;
        }

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        JobBuilder.exitJVM(new MaxTemperatureUsingSecondarySort(), args);
    }
}
