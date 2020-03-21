package bd.study.code.hadoop.mapreduce.chapter9;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * 读取顺序文件
 *
 * @author 努力常态化
 * @date 2020/3/19 21:49
 */
public class SequenceFileReadDemo {
    public static void main(String[] args) throws IOException {
//        String uri = args[0];
//        String uri = "D:\\workDir\\otherFile\\temp\\airtemperature_data\\testoutput2\\part-r-00000";
        String uri = "D:\\workDir\\otherFile\\temp\\airtemperature_data\\testoutput\\part-r-00000";
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(uri);
        SequenceFile.Reader reader = null;

        try {
            reader = new SequenceFile.Reader(fileSystem, path, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            while (reader.next(key, value)) {
                System.out.printf("%s\t%s\n", key, value);
            }
        } finally {
            IOUtils.closeStream(reader);
        }


    }
}
