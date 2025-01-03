package org.main;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class FTReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder realOutput = new StringBuilder();
        StringBuilder imagOutput = new StringBuilder();

        for (Text value : values) {
            // Predpostavljamo, da je vrednost v formatu: "REAL: <value>, IMAG: <value>"
            String[] parts = value.toString().split(","); // Razdeli na realni in imaginarni del
            if (parts.length == 2) {
                // Dodaj realni del
                realOutput.append(parts[0].trim()).append("\n");
                // Dodaj imaginarni del
                imagOutput.append(parts[1].trim()).append("\n");
            } else {
                // Če format ni pravilen, logiraj napako
                System.err.println("Invalid value format: " + value.toString());
            }
        }

        multipleOutputs.write(key, new Text(realOutput.toString()), "finalRealOutput");
        multipleOutputs.write(key, new Text(imagOutput.toString()), "finalImagOutput");

    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
