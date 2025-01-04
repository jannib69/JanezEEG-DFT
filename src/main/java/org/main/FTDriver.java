package org.main;

import lombok.var;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class FTDriver {
    private static final Logger LOGGER = Logger.getLogger(FTDriver.class.getName());

    public static void main(String[] args) throws Exception {
        // Setup logging to file
        setupLogger();

        if (args.length != 2) {
            LOGGER.severe("Usage: FTDriver <input path> <output path>");
            System.exit(-1);
        }

        LOGGER.info("Starting Hadoop job for DFT...");

        var conf = new Configuration();
        var job = Job.getInstance(conf, "Discrete Fourier Transform");

        job.setJarByClass(FTDriver.class);
        job.setMapperClass(FTMapper.class);
        job.setReducerClass(FTReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // Multiple outputs for various frequency bands
        MultipleOutputs.addNamedOutput(job, "realOutput", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "imagOutput", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "deltaOutput", TextOutputFormat.class, Text.class, Text.class); // Added for Delta
        MultipleOutputs.addNamedOutput(job, "thetaOutput", TextOutputFormat.class, Text.class, Text.class); // Added for Theta
        MultipleOutputs.addNamedOutput(job, "alphaOutput", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "betaOutput", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "gammaOutput", TextOutputFormat.class, Text.class, Text.class);

        LOGGER.info("Hadoop job configuration is set.");

        // Run job and check for completion
        if (job.waitForCompletion(true)) {
            LOGGER.info("Hadoop job completed successfully.");
            System.exit(0);
        } else {
            LOGGER.severe("Hadoop job failed.");
            System.exit(1);
        }
    }

    /**
     * Setup Logger for writing to a file.
     */
    private static void setupLogger() {
        try {
            var fileHandler = new FileHandler(new File("job_logs.log").getAbsolutePath(), true);
            fileHandler.setFormatter(new SimpleFormatter());

            LOGGER.addHandler(fileHandler);
            LOGGER.setLevel(Level.ALL);
        } catch (IOException e) {
            System.err.println("Failed to initialize logger: " + e.getMessage());
            System.exit(-1);
        }
    }
}
