package com.hakunamapdata.zlatanitor.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

/**
 *
 * @author kawaa
 */
public class ZlatanitorUtils {

    public static String LOGGER_LEVEL_NAME = "logger.level";
    public static String LOGGER_LEVEL_DEFAULT_VALUE = "info";
    public static String FS_COUNTER = "org.apache.hadoop.mapreduce.FileSystemCounter";
    public static NullWritable NULL_WRITABLE = NullWritable.get();
    public static IntWritable ONE = new IntWritable(1);

    public static Configuration setDefaultConfigurationOptions(Configuration conf) {
        long splitSize = 4 * 134217728;
        conf.set("avro.mapred.deflate.level", "6");
        conf.set("mapreduce.output.fileoutputformat.compress", "false");

        conf.setLong("mapreduce.input.fileinputformat.split.minsize", splitSize);
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", splitSize);
        conf.setLong("mapreduce.input.fileinputformat.split.minsize.per.node", splitSize);
        conf.setLong("mapreduce.input.fileinputformat.split.minsize.per.rack", splitSize);
        return conf;
    }
}
