package com.hadoop.limitations.driver;

import com.hadoop.limitations.mapper.FrequencyMapper;
import com.hadoop.limitations.mapper.RelabelMapper;
import com.hadoop.limitations.reducer.FrequencyReducer;
import com.hadoop.limitations.reducer.RelabelReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        Job counterJob = Job.getInstance(conf, "Counter Job");
//        counterJob.setJarByClass(Driver.class);
//        counterJob.setMapperClass(FrequencyMapper.class);
//        counterJob.setReducerClass(FrequencyReducer.class);
//        counterJob.setMapOutputKeyClass(Text.class);
//        counterJob.setOutputKeyClass(Text.class);
//        counterJob.setOutputValueClass(IntWritable.class);
//
//        FileInputFormat.addInputPath(counterJob, new Path(args[0]));
//        FileOutputFormat.setOutputPath(counterJob, new Path(args[1]));
//
//       boolean success = counterJob.waitForCompletion(true);
//
//       if(!success)
//           System.exit(1);

        Job relabelingJob = Job.getInstance(conf, "Relabeling Job");
        relabelingJob.setJarByClass(Driver.class);
        relabelingJob.setMapperClass(RelabelMapper.class);
        relabelingJob.setReducerClass(RelabelReducer.class);
        relabelingJob.setMapOutputKeyClass(IntWritable.class);
        relabelingJob.setOutputKeyClass(Text.class);
        relabelingJob.setOutputValueClass(Text.class);

        Path temp = new Path(args[1] + "/part-r-00000");

        relabelingJob.addCacheFile(new Path(args[1]).toUri());

        FileInputFormat.addInputPath(relabelingJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(relabelingJob, new Path(args[2]));

        boolean success = relabelingJob.waitForCompletion(true);

        if(!success)
            System.exit(1);

        System.exit(0);
    }
}
