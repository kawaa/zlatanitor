package com.hakunamapdata.zlatanitor.hadoop.mapreduce.lib.input;

import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

public class FilenameInputFormat
        extends FileInputFormat<Text, NullWritable> {

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
    
    @Override
    public RecordReader<Text, NullWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        FilenameRecordReader reader = new FilenameRecordReader();
        reader.initialize(split, context);
        return reader;
    }
}
