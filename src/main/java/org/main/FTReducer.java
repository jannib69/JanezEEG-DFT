package org.main;

import lombok.var;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.logging.Logger;

public class FTReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs<Text, Text> multipleOutputs;
    private static final Logger LOGGER = Logger.getLogger(FTReducer.class.getName());

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
        LOGGER.info("Reducer setup initialized.");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (key.toString().equals("LOG")) {
            LOGGER.warning("Unexpected LOG key in Reducer.");
            return;
        }

        LOGGER.info("Processing key: " + key.toString());
        var combinedReal = new StringBuilder();
        var combinedImag = new StringBuilder();

        for (var value : values) {
            var parts = value.toString().split("\\|");
            if (parts.length == 2) {
                combinedReal.append(parts[0]).append(",");
                combinedImag.append(parts[1]).append(",");
            }
        }

        if (combinedReal.length() > 0) combinedReal.setLength(combinedReal.length() - 1);
        if (combinedImag.length() > 0) combinedImag.setLength(combinedImag.length() - 1);

        var electrodeName = key.toString();
        multipleOutputs.write("realOutput", new Text(electrodeName), new Text(combinedReal.toString()));
        multipleOutputs.write("imagOutput", new Text(electrodeName), new Text(combinedImag.toString()));

        LOGGER.info("Combined real output for " + electrodeName + ": " + combinedReal);
        LOGGER.info("Combined imaginary output for " + electrodeName + ": " + combinedImag);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
        LOGGER.info("Reducer cleanup completed.");
    }
}
