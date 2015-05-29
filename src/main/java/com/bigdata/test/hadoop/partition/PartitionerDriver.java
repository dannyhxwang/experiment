package com.bigdata.test.hadoop.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by wanghaixing on 15-5-29.
 */
public class PartitionerDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if(args.length != 3) {
            System.err.println("Usage:\n PartitionerDriver <in> <out> <useTotalOrder>");
            return -1;
        }
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        String useTotalOrder = args[2];

        Configuration conf = getConf();
        out.getFileSystem(conf).delete(out, true);
        Job job = Job.getInstance(conf, "TotalOrderPartitioner");
        job.setJarByClass(PartitionerDriver.class);
        FileInputFormat.setInputPaths(job, in);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(PartitionerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        if(useTotalOrder != null && useTotalOrder.equals("true")) {
            job.setPartitionerClass(TotalOrderPartitioner.class);
            MyInputSampler.Sampler<Text, Text> sampler = new MyInputSampler.RandomSampler<Text, Text>(0.1, 20, 3);
            MyInputSampler.writePartitionFile(job, sampler);

            String partitionFile = TotalOrderPartitioner.getPartitionFile(getConf());
            URI partitionURI = new URI(partitionFile+"#"+TotalOrderPartitioner.DEFAULT_PATH);
            job.addCacheArchive(partitionURI);
        }

        job.setReducerClass(Reducer.class);
        FileOutputFormat.setOutputPath(job, out);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new PartitionerDriver(),args);
    }


}
