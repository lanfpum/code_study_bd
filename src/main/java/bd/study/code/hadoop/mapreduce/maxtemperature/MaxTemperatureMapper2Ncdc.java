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
 * @date 2020/3/1 21:02
 */
public class MaxTemperatureMapper2Ncdc extends Mapper<LongWritable, Text, Text, IntWritable> {
    enum Temperature {
        // 超过100度的温度
        OVER_100
    }

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);

        if (parser.isValidTemperature()) {
            int airTemperature = parser.getAirTemperature();

            if (airTemperature > 1000) {
                System.err.println("Temperature over 100 degrees for input : " + value);
                context.setStatus("Detected possible corrupt record : see logs.");
                context.getCounter(Temperature.OVER_100).increment(1);
            }

            context.write(new Text(parser.getYear()),
                    new IntWritable(airTemperature));
        }

    }
}
