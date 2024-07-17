package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, NullWritable> {

    private static final int HEIGHT_INDEX = 6;
    private boolean isFirstLine = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        String[] fields = value.toString().split(";");
        if (fields.length > HEIGHT_INDEX) {
            try {
                double height = Double.parseDouble(fields[HEIGHT_INDEX].trim());
                context.write(new DoubleWritable(height), NullWritable.get());
            } catch (NumberFormatException e) {
                // Ignore lines where the height is not a valid number
            }
        }
    }
}