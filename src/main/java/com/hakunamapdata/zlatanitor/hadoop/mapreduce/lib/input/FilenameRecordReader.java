package com.hakunamapdata.zlatanitor.hadoop.mapreduce.lib.input;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


class FilenameRecordReader extends RecordReader<Text, NullWritable> {

    private FileSplit fileSplit;
    private Text key = new Text();
    private NullWritable value = NullWritable.get();
    private boolean processed = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            Path file = fileSplit.getPath();
            key.set(file.toString());
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public NullWritable getCurrentValue() throws IOException,
            InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}