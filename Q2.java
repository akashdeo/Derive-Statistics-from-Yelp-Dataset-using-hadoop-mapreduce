/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.hw1q2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author akash
 */
public class Q2 {

    public static class mapIt extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String nextLine;
            String[] lineArray;
            String[] friendList;

            nextLine = value.toString();
            //System.out.println(nextLine);
            lineArray = nextLine.split("\t");
            //System.out.println(lineArray[0]);
            if (lineArray.length == 1) {
            } else {
                //System.out.println(lineArray[0]);
                friendList = lineArray[1].split(",");
                //System.out.println("FRIENDS: "+lineArray[1]);
                int[] mutualFriend = new int[2];
                for (int len = 0; len < friendList.length; len++) {
                    mutualFriend[0] = Integer.parseInt(lineArray[0]);
                    mutualFriend[1] = Integer.parseInt(friendList[len]);
                    Arrays.sort(mutualFriend);
                    context.write(new Text(mutualFriend[0] + "," + mutualFriend[1]), new Text(lineArray[1]));
                }
            }
        }

    }

    public static class reduceIt extends Reducer<Text, Text, Text, Text> {

        HashMap<String, Integer> hmap = new HashMap<>();
        
        /**
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text[] texts = new Text[2];
            int index = 0;
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                texts[index++] = new Text(iterator.next());
            }
            String[] friendListA = texts[0].toString().split(",");
            String[] friendListB = texts[1].toString().split(",");
            List<String> list = new LinkedList<>();
            for (String friend1 : friendListA) {
                for (String friend2 : friendListB) {
                    if (friend1.equals(friend2)) {
                        list.add(friend1);
                    }
                }
            }
            if (list.size() > 0) {
                hmap.put(key.toString(), list.size());
            }

        }

        public <K, V extends Comparable<? super V>> Map<K, V> sortByDescendingValue(Map<K, V> map) {
            return map.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (e1, e2) -> e1,
                            LinkedHashMap::new
                    ));
        }

        /**
         *
         * @param context
         * @throws java.io.IOException
         * @throws java.lang.InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            hmap = (HashMap)sortByDescendingValue(hmap);
            int i = 0;
            for (Map.Entry<String, Integer> entry : hmap.entrySet()) {
                if (i == 10) {
                    break;
                }
                context.write(new Text(entry.getKey()+""), new Text(entry.getValue() + ""));
                i++;
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "top 10  Friends");
        job.setJarByClass(Q2.class);
        job.setMapperClass(mapIt.class);
        job.setReducerClass(reduceIt.class);

        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
