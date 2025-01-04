package org.main;

import lombok.var;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

public class FTMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int SEGMENT_LENGTH = 2048 * 2; // Segment length
    private static final Logger LOGGER = Logger.getLogger(FTMapper.class.getName());
    private static final int FS = 500; // Sampling frequency (500 Hz for EEG data)

    @Override
    protected void setup(Context context) {
        LOGGER.info("Mapper setup initialized.");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LOGGER.info("Processing line: " + value.toString());

        // Split the line into columns
        var columns = value.toString().split(",");
        if (columns.length < 2) {
            LOGGER.warning("Skipping line due to insufficient columns: " + value.toString());
            return;
        }

        String electrodeName = columns[0];
        double[] signal = parseSignal(columns);
        int N = signal.length;

        if (N < SEGMENT_LENGTH) {
            LOGGER.warning("Signal is shorter than segment length: " + N);
            return;
        }

        double freqResolution = (double) FS / SEGMENT_LENGTH;

        // Frequency bins for Delta (0.5-4 Hz), Theta (4-8 Hz), Alpha (8-12 Hz), Beta (13-30 Hz), Gamma (30-100 Hz)
        int deltaStart = (int) Math.floor(0.5 / freqResolution);
        int deltaEnd = (int) Math.ceil(4 / freqResolution);
        int thetaStart = (int) Math.floor(4 / freqResolution);
        int thetaEnd = (int) Math.ceil(8 / freqResolution);
        int alphaStart = (int) Math.floor(8 / freqResolution);
        int alphaEnd = (int) Math.ceil(12 / freqResolution);
        int betaStart = (int) Math.floor(13 / freqResolution);
        int betaEnd = (int) Math.ceil(30 / freqResolution);
        int gammaStart = (int) Math.floor(30 / freqResolution);
        int gammaEnd = (int) Math.ceil(100 / freqResolution);

        LOGGER.info("Delta bins range: " + deltaStart + " to " + deltaEnd);
        LOGGER.info("Theta bins range: " + thetaStart + " to " + thetaEnd);
        LOGGER.info("Alpha bins range: " + alphaStart + " to " + alphaEnd);
        LOGGER.info("Beta bins range: " + betaStart + " to " + betaEnd);
        LOGGER.info("Gamma bins range: " + gammaStart + " to " + gammaEnd);

        for (int start = 0; start < N; start += SEGMENT_LENGTH) {
            int end = Math.min(start + SEGMENT_LENGTH, N);
            var segment = Arrays.copyOfRange(signal, start, end);
            LOGGER.info("Processing segment [" + start + ":" + end + "] for " + electrodeName);

            var realParts = new double[segment.length];
            var imagParts = new double[segment.length];

            for (var k = 0; k < segment.length; k++) {
                var sumReal = 0.0;
                var sumImag = 0.0;
                for (var n = 0; n < segment.length; n++) {
                    var angle = 2.0 * Math.PI * k * n / segment.length;
                    sumReal += segment[n] * Math.cos(angle);
                    sumImag -= segment[n] * Math.sin(angle);
                }
                realParts[k] = sumReal;
                imagParts[k] = sumImag;
            }

            // Calculate power in frequency bands
            double deltaPower = calculateBandPower(realParts, imagParts, deltaStart, deltaEnd);
            double thetaPower = calculateBandPower(realParts, imagParts, thetaStart, thetaEnd);
            double alphaPower = calculateBandPower(realParts, imagParts, alphaStart, alphaEnd);
            double betaPower = calculateBandPower(realParts, imagParts, betaStart, betaEnd);
            double gammaPower = calculateBandPower(realParts, imagParts, gammaStart, gammaEnd);

            LOGGER.info(String.format("Computed Power -> Delta: %.2f, Theta: %.2f, Alpha: %.2f, Beta: %.2f, Gamma: %.2f",
                    deltaPower, thetaPower, alphaPower, betaPower, gammaPower));

            // Output format: Real part | Imaginary part | Delta | Theta | Alpha | Beta | Gamma
            context.write(new Text(electrodeName), new Text(String.join("|",
                    Arrays.toString(realParts).replace("[", "").replace("]", "").replace(" ", ""),
                    Arrays.toString(imagParts).replace("[", "").replace("]", "").replace(" ", ""),
                    String.valueOf(deltaPower), String.valueOf(thetaPower), String.valueOf(alphaPower),
                    String.valueOf(betaPower), String.valueOf(gammaPower))));
        }
    }

    /**
     * Parse the signal from the input data.
     *
     * @param columns The columns of the input data
     * @return The signal as a double array
     */
    private double[] parseSignal(String[] columns) {
        var signal = new double[columns.length - 1]; // Columns containing the signal
        for (var i = 1; i < columns.length; i++) {
            try {
                signal[i - 1] = Double.parseDouble(columns[i].trim());
            } catch (NumberFormatException e) {
                LOGGER.warning("Invalid number format at column " + i + ": " + columns[i]);
                signal[i - 1] = 0.0;
            }
        }
        return signal;
    }

    /**
     * Calculate the average power of the signal for a given frequency band.
     *
     * @param realParts Real parts of the DFT
     * @param imagParts Imaginary parts of the DFT
     * @param start     The start bin (inclusive)
     * @param end       The end bin (inclusive)
     * @return The average power for the frequency band
     */
    private double calculateBandPower(double[] realParts, double[] imagParts, int start, int end) {
        if (start >= realParts.length) {
            LOGGER.warning("Start index exceeds realParts length: " + start);
            return 0.0;
        }
        double powerSum = 0.0;
        for (int k = start; k <= end && k < realParts.length; k++) {
            powerSum += Math.sqrt(realParts[k] * realParts[k] + imagParts[k] * imagParts[k]);
        }
        return powerSum / Math.max(1, (end - start + 1));
    }
}
