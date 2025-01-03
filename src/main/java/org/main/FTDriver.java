package org.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FTDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: FTDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Discrete Fourier Transform");

        // Fine-tune Hadoop configurations for large datasets
        conf.setInt("mapreduce.input.fileinputformat.split.minsize", 64 * 1024 * 1024); // 64MB
        conf.setInt("mapreduce.input.fileinputformat.split.maxsize", 128 * 1024 * 1024); // 128MB
        conf.setInt("mapreduce.map.memory.mb", 4096); // Set mapper memory to 4GB
        conf.set("mapreduce.map.java.opts", "-Xmx3584m"); // 90% of mapper memory
        conf.setInt("mapreduce.reduce.memory.mb", 4096); // Set reducer memory to 4GB
        conf.set("mapreduce.reduce.java.opts", "-Xmx3584m");

        // Optimize I/O performance
        conf.setInt("mapreduce.task.io.sort.mb", 512); // Increased buffer size
        conf.setFloat("mapreduce.map.sort.spill.percent", 0.85f); // Adjusted spill threshold
        conf.setInt("mapreduce.task.timeout", 600000); // Increase timeout to 10 minutes

        // Enable JVM reuse for better performance
        conf.setInt("mapreduce.job.jvm.numtasks", -1);

        // Disable speculative execution for consistency in processing
        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);

        // Job settings
        job.setJarByClass(FTDriver.class);
        job.setMapperClass(FTMapper.class);
        job.setReducerClass(FTReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Configure input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Calculate input size and set reducers
        long inputSize = new Path(args[0]).getFileSystem(conf).getContentSummary(new Path(args[0])).getLength();
        int reducerCount = Math.max(1, (int) (inputSize / (128 * 1024 * 1024))); // 1 reducer per 128MB
        job.setNumReduceTasks(reducerCount);

        MultipleOutputs.addNamedOutput(job, "finalRealOutput", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "finalImagOutput", TextOutputFormat.class, Text.class, Text.class);

        // Job execution
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
