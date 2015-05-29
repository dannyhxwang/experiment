package com.bigdata.test.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
    static final String INPUT_PATH = "hdfs://namenode:8020/input/hello";
    static final String OUTPUT_PATH = "hdfs://namenode:8020/out";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();

        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        if(fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH), true);
        }

        Job job = new Job(conf, WordCount.class.getSimpleName());
        job.setJarByClass(WordCount.class);

        FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);
        job.waitForCompletion(true);
    }

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable k1, Text v1, Context ctx) throws IOException ,InterruptedException {
            String[] splited = v1.toString().split("\t");
            for (String word : splited) {
                ctx.write(new Text(word), new LongWritable(1L));
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text k2, Iterable<LongWritable> values, Context ctx) throws IOException ,InterruptedException {
            long sum = 0L;
            for (LongWritable value : values) {
                sum += value.get();
            }
            ctx.write(k2, new LongWritable(sum));
        }
    }

}
