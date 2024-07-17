package com.opstty.mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;


public class SpeciesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int SPECIES_INDEX = 3;
    private final static IntWritable one = new IntWritable(1);
    private boolean isFirstLine = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        String[] fields = value.toString().split(";");
        if (fields.length > SPECIES_INDEX) {
            context.write(new Text(fields[SPECIES_INDEX].trim()), one);
        }
    }
}
