package com.hakunamapdata.zlatanitor.job.yarn.mapreduce;

import com.hakunamapdata.zlatanitor.hadoop.mapreduce.lib.input.CombineFilenameInputFormat;
import com.hakunamapdata.zlatanitor.job.yarn.mapper.ApplicationLogLineParserMapper;
import com.hakunamapdata.zlatanitor.utils.ZlatanitorUtils;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *
 * @author kawaa
 */
public class ApplicationLogLineCount extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        ZlatanitorUtils.setDefaultConfigurationOptions(conf);

        Job job = Job.getInstance(conf);
        job.setJarByClass(ApplicationLogLineCount.class);
        job.setJobName("ApplicationLogLineCount: " + StringUtils.join(args, " "));

        // set input
        MultipleInputs.addInputPath(job, new Path(args[0]),
                CombineFilenameInputFormat.class, ApplicationLogLineParserMapper.class);

        // set ouput
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, Text.class, IntWritable.class);

        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // submit a job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static String[] getLocalTestArgs() {
        String input = "src/test/resources/app-logs/application_1403384163948_0001/*";
        String output = "src/test/resources/app-logs-out";
        String[] testArgs = {input, output};
        return testArgs;
    }

    public static void main(String[] args) throws Exception {

        if (args == null || args.length == 0) {
            args = getLocalTestArgs();
            FileUtils.deleteDirectory(new File(args[1]));
        }

        int exitCode = ToolRunner.run(new ApplicationLogLineCount(), args);
        System.exit(exitCode);
    }
}
