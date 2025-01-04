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

    @Override
    protected void setup(Context context)  {
        LOGGER.info("Mapper setup initialized.");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LOGGER.info("Processing line: " + value.toString());

        var columns = value.toString().split(",");
        if (columns.length < 2) {
            LOGGER.warning("Skipping line due to insufficient columns: " + value.toString());
            return;
        }

        String electrodeName = columns[0];
        double[] signal = parseSignal(columns);

        var N = signal.length;
        LOGGER.info("Parsed electrode: " + electrodeName + ", Signal length: " + N);

        for (int start = 0; start < N; start += SEGMENT_LENGTH) {
            int end = Math.min(start + SEGMENT_LENGTH, N);
            var segment = Arrays.copyOfRange(signal, start, end);
            LOGGER.info("Processing segment [" + start + ":" + end + "] for " + electrodeName + ": " + Arrays.toString(segment));

            var realParts = new double[segment.length];
            var imagParts = new double[segment.length];

            // Izračun DFT
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

            LOGGER.info("DFT results for segment [" + start + ":" + end + "] - Real: " + Arrays.toString(realParts) + ", Imag: " + Arrays.toString(imagParts));

            String realOutput = Arrays.toString(realParts).replace("[", "").replace("]", "").replace(" ", "");
            String imagOutput = Arrays.toString(imagParts).replace("[", "").replace("]", "").replace(" ", "");

            context.write(new Text(electrodeName), new Text(realOutput + "|" + imagOutput));
        }
    }

    private double[] parseSignal(String[] columns) {
        var signal = new double[columns.length - 1];
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
}
