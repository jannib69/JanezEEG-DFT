package org.main;

import lombok.var;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

public class FTMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int SEGMENT_LENGTH = 2048;
    private static final Logger LOGGER = Logger.getLogger(FTMapper.class.getName());
    private static final int FS = 500;

    @Override
    protected void setup(Context context) {
        LOGGER.info("Mapper setup initialized.");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        var line = value.toString().trim();
        LOGGER.info("Processing line: " + line);

        var columns = line.split(",");
        if (columns.length < 2) {
            LOGGER.warning("Skipping line due to insufficient columns: " + line);
            return;
        }

        var electrodeName = columns[0];
        var signal = parseSignal(columns);
        if (signal.length < SEGMENT_LENGTH) {
            LOGGER.warning("Skipping line: signal is shorter than segment length");
            return;
        }

        double freqResolution = (double) FS / SEGMENT_LENGTH;

        // Frequency bins for EEG bands
        int deltaStart = (int) Math.floor(0.5 / freqResolution);
        int deltaEnd = (int) Math.ceil(4 / freqResolution);
        int thetaStart = (int) Math.floor(4 / freqResolution);
        int thetaEnd = (int) Math.ceil(8 / freqResolution);
        int alphaStart = (int) Math.floor(8 / freqResolution);
        int alphaEnd = (int) Math.ceil(14 / freqResolution);
        int betaStart = (int) Math.floor(14 / freqResolution);
        int betaEnd = (int) Math.ceil(30 / freqResolution);
        int gammaStart = (int) Math.floor(30 / freqResolution);
        int gammaEnd = (int) Math.ceil(100 / freqResolution);

        // Process each segment
        for (int start = 0; start < signal.length; start += SEGMENT_LENGTH) {
            int end = Math.min(start + SEGMENT_LENGTH, signal.length);
            var segment = Arrays.copyOfRange(signal, start, end);

            var realParts = new double[segment.length];
            var imagParts = new double[segment.length];

            // DFT calculation for the segment
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

            var delta = calculateBandPower(realParts, imagParts, deltaStart, deltaEnd);
            var theta = calculateBandPower(realParts, imagParts, thetaStart, thetaEnd);
            var alpha = calculateBandPower(realParts, imagParts, alphaStart, alphaEnd);
            var beta = calculateBandPower(realParts, imagParts, betaStart, betaEnd);
            var gamma = calculateBandPower(realParts, imagParts, gammaStart, gammaEnd);

            double totalPower = delta + theta + alpha + beta + gamma;

            double normDelta = delta / totalPower;
            double normTheta = theta / totalPower;
            double normAlpha = alpha / totalPower;
            double normBeta = beta / totalPower;
            double normGamma = gamma / totalPower;

            context.write(new Text(electrodeName),
                    new Text(normDelta + "," + normTheta + "," + normAlpha + "," + normBeta + "," + normGamma));
        }
    }

    private double[] parseSignal(String[] columns) {
        double[] signal = new double[columns.length - 1];
        for (int i = 1; i < columns.length; i++) {
            try {
                signal[i - 1] = Double.parseDouble(columns[i]);
            } catch (NumberFormatException e) {
                signal[i - 1] = 0.0;
            }
        }
        return signal;
    }

    private double calculateBandPower(double[] realParts, double[] imagParts, int start, int end) {
        double sum = 0.0;
        for (var k = start; k <= end && k < realParts.length; k++) {
            sum += Math.sqrt(realParts[k] * realParts[k] + imagParts[k] * imagParts[k]);
        }
        return sum / (end - start + 1);
    }
}