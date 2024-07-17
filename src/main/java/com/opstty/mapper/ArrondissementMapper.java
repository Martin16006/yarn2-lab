package com.opstty.mapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;


public class ArrondissementMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static final int ARRONDISSEMENT_INDEX = 1;

    private boolean isFirstLine = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        String[] fields = value.toString().split(";");

        if (fields.length > ARRONDISSEMENT_INDEX) {
            context.write(new Text(fields[ARRONDISSEMENT_INDEX].trim()), NullWritable.get());
        }
    }
}