package bd.study.code.hadoop.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The world always makes way for the dreamer
 * 根据文件扩展名解压数据
 *
 * @author 努力常态化
 * @date 2020/2/25 16:04
 */
public class FileDecompressor {
    public static void main(String[] args) throws IOException {
        String uri = "D:/workDir/demo_work/zip.bz2";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        FileSystem fileSystem = FileSystem.get(conf);
        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(inputPath);

        if (codec == null) {
            System.err.println("No codec found for " + uri);
            System.exit(1);
        }

        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
        InputStream inputStream = codec.createInputStream(fileSystem.open(inputPath));
        OutputStream outputStream = fileSystem.create(new Path(outputUri));
        IOUtils.copyBytes(inputStream, outputStream, conf);
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);
    }
}
