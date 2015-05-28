package com.bigdata.test.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class WordCountOld {
    static final String INPUT_PATH = "hdfs://namenode:8020/input/hello";
    static final String OUTPUT_PATH = "hdfs://namenode:8020/out";

    /**
     * 改动：
     * 1.不再使用Job，而是使用JobConf
     * 2.类的包中不再使用mapreduce，而是使用mapred
     * 3.不再使用job.waitForCompletion(true)提交作业，而是使用JobClient.runJob(job);
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();

        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        if(fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH), true);
        }


        JobConf job = new JobConf(conf, WordCountOld.class);

        //1.1 输入目录在哪里
        FileInputFormat.setInputPaths(job, INPUT_PATH);
        //指定对输入数据进行格式化处理的类
        job.setInputFormat(TextInputFormat.class);

        //1.2 指定自义的Mapper类
        job.setMapperClass(MyMapper.class);
        //指定map输出的<k,v>类型。如果<k3,v3>的类型与<k2,v2>的类型一致，那么可以省略
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(LongWritable.class);

        //1.3 分区
//		job.setPartitionerClass(HashPartitioner.class);
//		job.setNumReduceTasks(1);

        //1.4 TODO 排序、分组

        //1.5 TODO （可选）归约

        //2.2 指定自定义的Reduce类
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //2.3 指定输出 的路径
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        //指定输出的格式化类
//		job.setOutputFormatClass(TextOutputFormat.class);

        //把作业提交给JobTracker运行
        JobClient.runJob(job);
    }

    static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, LongWritable> collector, Reporter reporter)
                throws IOException {
            final String[] splited = value.toString().split("\t");
            for(String word : splited) {
                collector.collect(new Text(word), new LongWritable(1L));
            }
        }
    }

    static class MyReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterator<LongWritable> v2s,
                           OutputCollector<Text, LongWritable> collector, Reporter reporter)
                throws IOException {
            long times = 0;
            while(v2s.hasNext()) {
                times += v2s.next().get();
            }
            collector.collect(key, new LongWritable(times));
        }

    }

}