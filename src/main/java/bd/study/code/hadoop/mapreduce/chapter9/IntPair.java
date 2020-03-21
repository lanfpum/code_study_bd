package bd.study.code.hadoop.mapreduce.chapter9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The world always makes way for the dreamer
 *
 * @author 努力常态化
 * @date 2020/3/21 20:56
 */
public class IntPair implements WritableComparable<IntPair> {
    private int year;
    private int temperature;

    public void set(int year, int temperature) {
        this.year = year;
        this.temperature = temperature;
    }

    public IntPair() {
    }

    public IntPair(IntWritable year, IntWritable temperature) {
        set(year.get(), temperature.get());
    }

    public IntPair(int year, int temperature) {
        set(year, temperature);
    }

    public int getYear() {
        return year;
    }

    public int getTemperature() {
        return temperature;
    }

    public int compareTo(IntPair o) {
        int yearO = o.getYear();
        int tempO = o.getTemperature();

        if (year == yearO) {
            return -(temperature - tempO);
        } else {
            return year - yearO;
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(temperature);
    }

    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        temperature = in.readInt();
    }

    @Override
    public String toString() {
        return "IntPair{" +
                "year=" + year +
                ", temperature=" + temperature +
                '}';
    }
}
