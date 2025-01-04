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
        // Nastavi logiranje v datoteko
        setupLogger();

        if (args.length != 2) {
            LOGGER.severe("Usage: FTDriver <input path> <output path>");
            System.exit(-1);
        }

        LOGGER.info("Začenjanje Hadoop joba za DFT...");

        var conf = new Configuration();
        var job = Job.getInstance(conf, "Discrete Fourier Transform");

        job.setJarByClass(FTDriver.class);
        job.setMapperClass(FTMapper.class);
        job.setReducerClass(FTReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "realOutput", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "imagOutput", TextOutputFormat.class, Text.class, Text.class);

        LOGGER.info("Hadoop job konfiguracija je pripravljena.");

        if (job.waitForCompletion(true)) {
            LOGGER.info("Hadoop job je uspešno zaključen.");
            System.exit(0);
        } else {
            LOGGER.severe("Hadoop job je spodletel.");
            System.exit(1);
        }
    }

    private static void setupLogger() {
        try {
            var fileHandler = new FileHandler(new File("job_logs.log").getAbsolutePath(), true);
            fileHandler.setFormatter(new SimpleFormatter());

            LOGGER.addHandler(fileHandler);
            LOGGER.setLevel(Level.ALL);
        } catch (IOException e) {
            System.err.println("Neuspešna inicializacija loggerja: " + e.getMessage());
            System.exit(-1);
        }
    }
}
