package com.opstty.mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KindMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int KIND_INDEX = 2;
    private final static IntWritable one = new IntWritable(1);
    private boolean isFirstLine = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        String[] fields = value.toString().split(";");
        if (fields.length > KIND_INDEX) {
            context.write(new Text(fields[KIND_INDEX].trim()), one);
        }
    }
}
