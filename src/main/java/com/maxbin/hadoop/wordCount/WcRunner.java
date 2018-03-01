package com.maxbin.hadoop.wordCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WcRunner {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		Job wcjob = Job.getInstance(conf);
		
		//设置整个job所用的那些类在哪个jar包
		wcjob.setJarByClass(WcRunner.class);
		
		//本job使用的map类和reducer类
		wcjob.setMapperClass(WcMapper.class);
		wcjob.setReducerClass(WcReducer.class);
		
		//指定reducer的输出数据kv类型
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(LongWritable.class);
		
		//指定mapper的输出数据kv类型
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(LongWritable.class);
		
		//指定要处理的输入数据存放路径
		FileInputFormat.setInputPaths(wcjob, new Path("/wc/srcdata/"));
		
		//指定处理结果的输出数据存放路径
		FileOutputFormat.setOutputPath(wcjob, new Path("/wc/output"));
		
		wcjob.waitForCompletion(true);
		
		
		
	}

}
