package bd.study.code.hadoop.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * The world always makes way for the dreamer
 *
 * @author 努力常态化
 * @date 2020/2/25 11:31
 */
public class StreamCompressor {
    public static void main(String[] args) throws IOException {
        Class[] zipClasses = {
                DeflateCodec.class,
                GzipCodec.class,
                BZip2Codec.class,
                Lz4Codec.class
//                SnappyCodec.class
        };
        String outDir = "D:\\workDir\\demo_work\\zip";
        String inputFile = "D:\\workDir\\demo_work\\zip.txt";

        for (Class zipClass : zipClasses) {
            zip(zipClass, outDir, inputFile);
            unZip(zipClass,outDir);
        }

    }

    private static void zip(Class codesClass, String outDir, String inputFile) throws IOException {
        Configuration conf = new Configuration();
        CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(codesClass, conf);
        CompressionOutputStream out = compressionCodec.createOutputStream(new FileOutputStream(outDir
                + compressionCodec.getDefaultExtension()));
        IOUtils.copyBytes(new FileInputStream(inputFile), out, 4096, false);
        out.close();
//        out.finish();
    }

    private static void unZip(Class codesClass, String inputFile) throws IOException {
        System.out.println("------------------------------------------------------------------");
        Configuration conf = new Configuration();
        CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(codesClass, conf);
        CompressionInputStream inputStream = compressionCodec.createInputStream(new FileInputStream(inputFile
                + compressionCodec.getDefaultExtension()));
        IOUtils.copyBytes(inputStream, System.out, 4096);
        inputStream.close();
    }
}
