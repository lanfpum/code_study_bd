package bd.study.code.hadoop.mapreduce.chapter9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The world always makes way for the dreamer
 *
 * @author 努力常态化
 * @date 2020/3/22 21:21
 */
public class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(Text first, Text second) {
        set(first, second);
    }

    public TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public int compareTo(TextPair o) {
        int cmp = first.compareTo(o.first);

        if (cmp != 0) {
            return cmp;
        }

        return second.compareTo(o.second);
    }

    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public String toString() {
        return "TextPair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
