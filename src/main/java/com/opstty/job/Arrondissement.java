package com.opstty.job;

import com.opstty.mapper.ArrondissementMapper;
import com.opstty.reducer.ArrondissementReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Arrondissement {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ArrondissementDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Distinct Arrondissements");

        job.setJarByClass(Arrondissement.class);
        job.setMapperClass(ArrondissementMapper.class);
        job.setReducerClass(ArrondissementReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
