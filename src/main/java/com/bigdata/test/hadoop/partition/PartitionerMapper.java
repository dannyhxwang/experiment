package com.bigdata.test.hadoop.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wanghaixing on 15-5-29.
 */
public class PartitionerMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable k1, Text v1, Context ctx) throws IOException, InterruptedException {
        String[] splited = v1.toString().split("_");
        if(splited.length != 2) {
            return;
        }
        Text k2 = new Text(splited[1]);
        Text v2 = new Text(splited[0]);
        ctx.write(k2, v2);
    }
}
