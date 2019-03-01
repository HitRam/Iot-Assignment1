package com.hadoop.limitations.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RelabelReducer extends Reducer<IntWritable, Text, Text, Text > {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Text empty = new Text();
        empty.set("");
        for(Text value: values)
            context.write(empty, value);
    }
}
