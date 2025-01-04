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

        // Clean up the key (electrode) by trimming unnecessary characters
        var sanitizedKey = key.toString().trim();
        LOGGER.info("Processing key: " + sanitizedKey);

        // Prepare the combined results using StringBuilders
        var combinedReal = new StringBuilder();
        var combinedImag = new StringBuilder();
        var combinedDelta = new StringBuilder();
        var combinedTheta = new StringBuilder();
        var combinedAlpha = new StringBuilder();
        var combinedBeta = new StringBuilder();
        var combinedGamma = new StringBuilder();

        // Columns will be counted to handle headers
        int columnIndex = 1;

        // Add values while cleaning them
        for (var value : values) {
            var sanitizedValue = value.toString().trim(); // Clean extra spaces or unwanted characters
            LOGGER.info("Processing value: " + sanitizedValue);

            var parts = sanitizedValue.split("\\|"); // Expect values to be split by '|'

            // Ensure proper population of the components with default values for missing entries
            combinedReal.append(parts.length > 0 ? parts[0].trim() : "0").append(",");
            combinedImag.append(parts.length > 1 ? parts[1].trim() : "0").append(",");
            combinedDelta.append(parts.length > 2 ? parts[2].trim() : "0").append(",");
            combinedTheta.append(parts.length > 3 ? parts[3].trim() : "0").append(",");
            combinedAlpha.append(parts.length > 4 ? parts[4].trim() : "0").append(",");
            combinedBeta.append(parts.length > 5 ? parts[5].trim() : "0").append(",");
            combinedGamma.append(parts.length > 6 ? parts[6].trim() : "0").append(",");

            columnIndex++;
        }

        // Remove trailing commas
        trimTrailingComma(combinedReal);
        trimTrailingComma(combinedImag);
        trimTrailingComma(combinedDelta);
        trimTrailingComma(combinedTheta);
        trimTrailingComma(combinedAlpha);
        trimTrailingComma(combinedBeta);
        trimTrailingComma(combinedGamma);

        // Log processed data for each band
        LOGGER.info("Combined Real: " + combinedReal.toString());
        LOGGER.info("Combined Imag: " + combinedImag.toString());
        LOGGER.info("Combined Delta: " + combinedDelta.toString());
        LOGGER.info("Combined Theta: " + combinedTheta.toString());
        LOGGER.info("Combined Alpha: " + combinedAlpha.toString());
        LOGGER.info("Combined Beta: " + combinedBeta.toString());
        LOGGER.info("Combined Gamma: " + combinedGamma.toString());

        // Prepare the header row with column labels
        var header = new StringBuilder("Electrode");
        for (int i = 1; i < columnIndex; i++) {
            header.append(",Column").append(i);
        }

        // Write header row only once to output files (ensure unique write)
        if (context.getCounter("custom", "headerWritten").getValue() == 0) {
            LOGGER.info("Writing header to output files: " + header.toString());

            multipleOutputs.write("realOutput", new Text(header.toString()), new Text(""));
            multipleOutputs.write("imagOutput", new Text(header.toString()), new Text(""));
            multipleOutputs.write("deltaOutput", new Text(header.toString()), new Text(""));
            multipleOutputs.write("thetaOutput", new Text(header.toString()), new Text(""));
            multipleOutputs.write("alphaOutput", new Text(header.toString()), new Text(""));
            multipleOutputs.write("betaOutput", new Text(header.toString()), new Text(""));
            multipleOutputs.write("gammaOutput", new Text(header.toString()), new Text(""));

            // Mark that header has been written
            context.getCounter("custom", "headerWritten").increment(1);
        }

        // Add the electrode name to the output
        var electrodeName = sanitizedKey + ",";

        // Write combined results for real and imaginary parts
        multipleOutputs.write("realOutput", new Text(electrodeName), new Text(combinedReal.toString()));
        multipleOutputs.write("imagOutput", new Text(electrodeName), new Text(combinedImag.toString()));

        // Write results for EEG frequency bands to separate output files
        multipleOutputs.write("deltaOutput", new Text(electrodeName), new Text(combinedDelta.toString()));
        multipleOutputs.write("thetaOutput", new Text(electrodeName), new Text(combinedTheta.toString()));
        multipleOutputs.write("alphaOutput", new Text(electrodeName), new Text(combinedAlpha.toString()));
        multipleOutputs.write("betaOutput", new Text(electrodeName), new Text(combinedBeta.toString()));
        multipleOutputs.write("gammaOutput", new Text(electrodeName), new Text(combinedGamma.toString()));

        LOGGER.info("Outputs written for electrode: " + sanitizedKey);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        LOGGER.info("Closing MultipleOutputs.");
        try {
            multipleOutputs.close();
        } catch (IOException e) {
            LOGGER.severe("Failed to close MultipleOutputs: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Utility method for trimming trailing commas from StringBuilder.
     */
    private void trimTrailingComma(StringBuilder sb) {
        while (sb.length() > 0 && (sb.charAt(sb.length() - 1) == ',' || sb.charAt(sb.length() - 1) == '\t')) {
            sb.setLength(sb.length() - 1);
        }
    }
}
