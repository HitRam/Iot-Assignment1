package com.hadoop.limitations.mapper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class RelabelMapper extends Mapper<Object, Text, IntWritable, Text> {

    private HashMap<Integer, ArrayList<String>> frequencyMap;
    private HashMap<String, Integer> relabelMap;


    @Override
    protected void setup(Context context) {
        try {
            frequencyMap = new HashMap<Integer, ArrayList<String>>();
            URI[] frequencies = context.getCacheFiles();
            if(frequencies != null && frequencies.length > 0) {
                for(URI frequency: frequencies) {
                    Path file = new Path(frequency);
                    System.out.println(file.toString());
                    BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
                    String line = null;
                    while((line = reader.readLine()) != null) {
                        String[] vals = line.split(" ");
                        int key = Integer.valueOf(vals[1]);
                        String val = vals[0];
                        frequencyMap.putIfAbsent(key, new ArrayList<String>());
                        frequencyMap.get(key).add(val);
                    }
                }

                ArrayList<Integer> sortedKeys = new ArrayList<>(frequencyMap.keySet());
                Collections.sort(sortedKeys, Collections.reverseOrder());
                relabelMap = new HashMap<>();
                int idx = 1;
                for(Integer key: sortedKeys) {
                    for (String val : frequencyMap.get(key)) {
                        relabelMap.put(val, idx);
                        idx++;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        Text row = new Text();
        ArrayList<Integer> temp = new ArrayList<>();
        int count = 0;
        while(tokenizer.hasMoreTokens()) {
            count++;
            String token = tokenizer.nextToken();
            temp.add(relabelMap.get(token));
        }

        Collections.sort(temp);
        String curr_row = "";
        for(Integer val: temp)
            curr_row += val.toString() + " ";

        row.set(curr_row);
        context.write(new IntWritable(-1 * count), row);
    }

}
