package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private static final int KIND_INDEX = 2;
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
            String kind = fields[KIND_INDEX].trim();
            double height;
            try {
                height = Double.parseDouble(fields[HEIGHT_INDEX].trim());
            } catch (NumberFormatException e) {
                return;
            }
            context.write(new Text(kind), new DoubleWritable(height));
        }
    }
}
