package org.main;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FTReducer extends Reducer<Text, Text, Text, Text> {

    private static boolean headerAdded = false;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (!headerAdded) {
            context.write(new Text("Elektroda,Delta,Theta,Alpha,Beta,Gamma"), null);
            headerAdded = true;
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalDelta = 0, totalTheta = 0, totalAlpha = 0, totalBeta = 0, totalGamma = 0;
        int count = 0;

        for (Text value : values) {
            String[] bands = value.toString().split(",");
            if (bands.length == 5) {
                totalDelta += Double.parseDouble(bands[0]);
                totalTheta += Double.parseDouble(bands[1]);
                totalAlpha += Double.parseDouble(bands[2]);
                totalBeta += Double.parseDouble(bands[3]);
                totalGamma += Double.parseDouble(bands[4]);
                count++;
            }
        }

        if (count > 0) {
            totalDelta /= count;
            totalTheta /= count;
            totalAlpha /= count;
            totalBeta /= count;
            totalGamma /= count;
        }

        String csvOutput = String.format("%s,%.2f,%.2f,%.2f,%.2f,%.2f",
                key.toString().trim(), totalDelta, totalTheta, totalAlpha, totalBeta, totalGamma);

        context.write(new Text(csvOutput), null);
    }
}