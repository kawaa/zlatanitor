/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hakunamapdata.zlatanitor.hadoop.mapreduce.lib.input;


import java.io.IOException;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CombineFilenameInputFormat extends CombineFileInputFormat<Text, NullWritable> {

    @Override
    public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader((CombineFileSplit) split, context, (Class) CombineWholeFileRecordReader.class);
    }

    public static class CombineWholeFileRecordReader extends RecordReader<Text, NullWritable> {

        private FilenameInputFormat inputFormat = new FilenameInputFormat();
        private final RecordReader<Text, NullWritable> recordReader;

        public CombineWholeFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException, InterruptedException {
            FileSplit filesplit = new FileSplit(split.getPath(index), split.getOffset(index), split.getLength(index), split.getLocations());
            recordReader = inputFormat.createRecordReader(filesplit, context);
        }

        @Override
        public void close() throws IOException {
            recordReader.close();
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return recordReader.nextKeyValue();
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return recordReader.getCurrentKey();
        }

        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {
            return recordReader.getCurrentValue();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return recordReader.getProgress();
        }
    }
}