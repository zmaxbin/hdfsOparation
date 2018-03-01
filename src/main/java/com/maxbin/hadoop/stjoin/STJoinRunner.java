package com.maxbin.hadoop.stjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class STJoinRunner extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(STJoinRunner.class);
		
		job.setMapperClass(STJoinMapper.class);
		job.setReducerClass(STJoinReducer.class);
		
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path path = new Path("E:\\output6");
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {
        	fileSystem.delete(path, true);
        }
		
		FileInputFormat.setInputPaths(job, new Path("E:\\stjoin.txt"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\output6"));
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new STJoinRunner(), args);
		System.exit(res);
	}
	

}
