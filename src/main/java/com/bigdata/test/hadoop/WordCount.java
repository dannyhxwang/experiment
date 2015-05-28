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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

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

        //1.1 输入目录在哪里
        FileInputFormat.setInputPaths(job, INPUT_PATH);
        //指定对输入数据进行格式化处理的类
//		job.setInputFormatClass(TextInputFormat.class);

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
        job.waitForCompletion(true);
    }

    /**
     * KEYIN	即k1		表示每一行的起始位置（偏移量offset）
     * VALUEIN	即v1		表示每一行的文本内容
     * KEYOUT	即k2		表示每一行中的每个单词
     * VALUEOUT	即v2		表示每一行中的每个单词的出现次数，固定值1
     */
    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        protected void map(LongWritable k1, Text v1, Context ctx) throws IOException ,InterruptedException {
            String[] splited = v1.toString().split("\t");
            for (String word : splited) {
                ctx.write(new Text(word), new LongWritable(1L));
            }
        }
    }

    /**
     * KEYIN	即k2		表示每一行中的每个单词
     * VALUEIN	即v2		表示每一行中每个单词的出现次数
     * KEYOUT	即k3		表示整个文件中的不同单词
     * VALUEOUT	即v3		表示整个文件中不同单词出现的总次数
     */
    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        protected void reduce(Text k2, Iterable<LongWritable> values, Context ctx) throws IOException ,InterruptedException {
            long sum = 0L;
            for (LongWritable value : values) {
                sum += value.get();
            }
            ctx.write(k2, new LongWritable(sum));
        }
    }

}
