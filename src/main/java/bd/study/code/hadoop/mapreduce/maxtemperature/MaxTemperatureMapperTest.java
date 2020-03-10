package bd.study.code.hadoop.mapreduce.maxtemperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.zip.CheckedOutputStream;


/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2020/3/1 20:39.
 */
public class MaxTemperatureMapperTest {
    @Test
    public void processesValiRecore() throws IOException {
        Text value = new Text("201232");
//        new MapDriver<LongWritable, Text, Text, IntWritable>()
//                .withMapper(new MaxTemperaTureMapper())
//                .withInput(new LongWritable(0), value)
////                .withOutput(new Text("2012"), new IntWritable(32))
//                .runTest();
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxTemperatureReducer())
                .withInput(new Text("1995"),
                        Arrays.asList(new IntWritable(22), new IntWritable(10)))
                .withOutput(new Text("1995"), new IntWritable(22))
                .runTest();
    }

    @Test
    public void test() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        Path inputPath = new Path("inputDir");
        Path outputPath = new Path("outputDir");

        FileSystem fileSystem = FileSystem.getLocal(conf);
        fileSystem.delete(outputPath, true);


    }
}
