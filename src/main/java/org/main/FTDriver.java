package org.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.logging.Logger;

public class FTDriver {
    private static final Logger LOGGER = Logger.getLogger(FTDriver.class.getName());

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: FTDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Discrete Fourier Transform");

        job.setJarByClass(FTDriver.class);
        job.setMapperClass(FTMapper.class);
        job.setReducerClass(FTReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (job.waitForCompletion(true)) {
            LOGGER.info("Job completed successfully.");
            System.exit(0);
        } else {
            LOGGER.severe("Job failed.");
            System.exit(1);
        }
    }
}