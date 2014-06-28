package com.hakunamapdata.zlatanitor.job.yarn.mapper;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import static com.hakunamapdata.zlatanitor.utils.ZlatanitorUtils.*;

/**
 *
 * @author kawaa
 */
public class ApplicationLogLineParserMapper extends Mapper<Text, NullWritable, Text, IntWritable> {

    // logger settings
    protected Logger LOGGER = Logger.getLogger(this.getClass().getName());
    protected Level level = null;
    protected FileSystem fs = null;
    // key and value
    private Text textKey = new Text();

    // read a key from the scanner
    public byte[] readKey(Scanner scanner) throws IOException {
        int keylen = scanner.entry().getKeyLength();
        byte[] read = new byte[keylen];
        scanner.entry().getKey(read);
        return read;
    }

    // read a value from the scanner
    public byte[] readValue(Scanner scanner) {
        try {
            int valueLen = scanner.entry().getValueLength();
            byte[] read = new byte[valueLen];
            scanner.entry().getValue(read);
            return read;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void setup(Mapper.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        level = Level.toLevel(conf.get(LOGGER_LEVEL_NAME, LOGGER_LEVEL_DEFAULT_VALUE));
        fs = FileSystem.get(conf);
    }

    @Override
    protected void map(Text key, NullWritable val, Mapper.Context context)
            throws IOException, InterruptedException {

        String location = key.toString();
        Path path = new Path(location);

        FSDataInputStream fin = fs.open(path);
        Reader reader = new Reader(fin, fs.getFileStatus(path).getLen(), context.getConfiguration());

        Scanner scanner = reader.createScanner();
        String line = null;
        while (!scanner.atEnd()) {
            byte[] value = readValue(scanner);
            line = (value != null ? new String(value) : null);
            scanner.advance();
        }

        if (line != null) {
            int start = line.indexOf("\u0006stderr");
            int stop = line.indexOf("\u0006stdout");
            if (start > 0 && stop > 0) {
                String error = line.substring(start, stop);
                String[] parts = error.split("\n");
                for (String part : parts) {
                    textKey.set(part);
                    context.write(textKey, ONE);
                }
            }
        }
    }
}