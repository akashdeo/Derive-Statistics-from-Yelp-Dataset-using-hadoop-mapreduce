/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.hw1q3;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author akash
 */
public class Q3 {

    public static class ReviewDetailsMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] values = value.toString().split("::");
            String businessId = values[2];
            String stars = values[3];
            context.write(new Text(businessId), new Text("rating:" + stars));

        }
    }

    public static class BusinessDetailsMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] values = value.toString().split("::");
            String businessId = values[0];
            String fullAddress = values[1];
            String categories = values[2];
            context.write(new Text(businessId), new Text("address:" + fullAddress + " ," + "categories_" + categories));
        }
    }

    public static class reducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            double count = 0.0;
            String address = "";
            for (Text value : values) {

                String[] tuple = value.toString().split(":");
                if (tuple[0].equals("rating")) {
                    sum += Double.parseDouble(tuple[1]);
                    count++;
                } else if (tuple[0].equals("address")) {
                    address += tuple[1];
                }
            }
            double average = (double) (sum / count);
            DoubleWritable avg = new DoubleWritable();
            avg.set(average);
            context.write(new Text(key), new Text("average:" + avg + "address:" + address));
        }

    }

    public static class mapIt extends
            Mapper<LongWritable, Text, DoubleWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String values[] = value.toString().split("average:");
            DoubleWritable keynew = new DoubleWritable();
            String[] details = values[1].split("address:");
            keynew.set(Double.parseDouble(details[0]));
            context.write(keynew, new Text(values[0] + ", Address_" + details[1]));

        }
    }

    public static class Top10 extends
            Reducer<DoubleWritable, Text, Text, Text> {

        int count = 0;

        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text v : values) {
                if (count == 10) {
                    break;
                }
                context.write(v, new Text(", rating_" + key.toString()));
                count++;
            }

        }

    }

    public static class SortDoubleComparator extends WritableComparator {

        //Constructor.
        public SortDoubleComparator() {
            super(DoubleWritable.class, true);
        }

        /**
         *
         * @param w1
         * @param w2
         * @return
         */
        @SuppressWarnings("rawtypes")

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable k1 = (DoubleWritable) w1;
            DoubleWritable k2 = (DoubleWritable) w2;

            return -1 * k1.compareTo(k2);
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = new Job(conf, "businessdetails");
        Path IntermediateoutputDir = new Path(otherArgs[2]);
        job.setJarByClass(Q3.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, ReviewDetailsMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, BusinessDetailsMapper.class);
        job.setReducerClass(reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, IntermediateoutputDir);
        int code = job.waitForCompletion(true) ? 0 : 1;

        Job job1 = new Job(conf, "topten");
        job1.setJarByClass(Q3.class);
        job1.setMapperClass(mapIt.class);
        job1.setReducerClass(Top10.class);
        job1.setMapOutputKeyClass(DoubleWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setSortComparatorClass(SortDoubleComparator.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, IntermediateoutputDir);
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3]));
        code = job1.waitForCompletion(true) ? 0 : 1;

    }

}
