package com.maxbin.hadoop.Hdfs2Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Hdfs2HbaseRunner extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        if(otherArgs.length != 2) {
	            System.err.println("Usage: wordcount <infile> <table>");
	            System.exit(2);
	        }
	        
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Hdfs2HbaseRunner.class);
		
		job.setMapperClass(Hdfs2HbaseMapper.class);
		job.setReducerClass(Hdfs2HbaseReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		
		
		job.setOutputFormatClass(TableOutputFormat.class);
		
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, otherArgs[1]);
		
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Hdfs2HbaseRunner(), args);
		System.exit(res);
	}
	
	

}
