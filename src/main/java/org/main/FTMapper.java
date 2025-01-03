package org.main;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Arrays;

public class FTMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Log LOG = LogFactory.getLog(FTMapper.class);
    private MultipleOutputs<Text, Text> multipleOutputs;
    private static final int SEGMENT_LENGTH = 1024;
    private static final double[] COS_TABLE = new double[SEGMENT_LENGTH];
    private static final double[] SIN_TABLE = new double[SEGMENT_LENGTH];

    static {
        for (int k = 0; k < SEGMENT_LENGTH; k++) {
            double angle = 2 * Math.PI * k / SEGMENT_LENGTH;
            COS_TABLE[k] = Math.cos(angle);
            SIN_TABLE[k] = Math.sin(angle);
        }
    }

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
        LOG.info("Mapper setup completed.");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");
        if (columns.length < 2) {
            LOG.warn("Skipping record due to insufficient columns: " + value);
            return;
        }

        String electrodeName = columns[0];
        double[] signal = parseSignal(columns);

        processSignal(electrodeName, signal);
    }

    private double[] parseSignal(String[] columns) {
        double[] signal = new double[columns.length - 1];
        for (int i = 1; i < columns.length; i++) {
            try {
                signal[i - 1] = Double.parseDouble(columns[i].trim());
            } catch (NumberFormatException e) {
                signal[i - 1] = 0.0;
                LOG.warn("Invalid number format at column " + i + ": " + columns[i]);
            }
        }
        return signal;
    }

    private void processSignal(String electrodeName, double[] signal) throws IOException, InterruptedException {
        int N = signal.length;
        for (int start = 0; start < N; start += SEGMENT_LENGTH) {
            int end = Math.min(start + SEGMENT_LENGTH, N);
            double[] segment = Arrays.copyOfRange(signal, start, end);
            int M = segment.length;



            double[] realParts = new double[M];
            double[] imagParts = new double[M];

            // Spremenjeno računanje DFT
            for (int k = 0; k < M; k++) {
                double sumReal = 0.0;
                double sumImag = 0.0;
                for (int n = 0; n < M; n++) {
                    double angle = 2.0 * Math.PI * k * n / M;
                    sumReal += segment[n] * Math.cos(angle);
                    sumImag -= segment[n] * Math.sin(angle);
                }
                // Normalizacija rezultatov
                realParts[k] = sumReal;
                imagParts[k] = sumImag;
            }



            String segmentName = electrodeName + "_segment_" + (start / SEGMENT_LENGTH);
            writeOutput(segmentName, realParts, imagParts);
        }
    }
    private void writeOutput(String segmentName, double[] realParts, double[] imagParts) throws IOException, InterruptedException {
        StringBuilder realBuilder = new StringBuilder();
        StringBuilder imagBuilder = new StringBuilder();

        for (int i = 0; i < realParts.length; i++) {
            realBuilder.append(realParts[i]).append(i < realParts.length - 1 ? "," : "");
            imagBuilder.append(imagParts[i]).append(i < imagParts.length - 1 ? "," : "");
        }

        multipleOutputs.write(new Text(segmentName), new Text(realBuilder.toString()), "electrode_real.csv");
        multipleOutputs.write(new Text(segmentName), new Text(imagBuilder.toString()), "electrode_imag.csv");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
        LOG.info("Mapper cleanup completed.");
    }
}
