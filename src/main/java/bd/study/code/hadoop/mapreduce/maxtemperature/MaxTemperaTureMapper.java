package bd.study.code.hadoop.mapreduce.maxtemperature;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 *
 * @author 努力常态化
 * @date 2020/3/1 20:33
 */
public class MaxTemperaTureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text year = new Text();
    IntWritable temperature = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String yearText = line.substring(0, 4);
        year.set(yearText);
        int ariTemperature = Integer.parseInt(line.substring(4));
        temperature.set(ariTemperature);
        context.write(year, temperature);
    }
}
