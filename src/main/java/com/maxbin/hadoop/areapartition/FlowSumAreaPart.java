package com.maxbin.hadoop.areapartition;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.maxbin.hadoop.flowsum.FlowBean;

/**
 * 对流量原始日志进行流量统计，将不同省份的用户统计结果输出到不同文件
 * 需要自定义改造两个机制：
 * 1、改造分区的逻辑
 * 2、自定义reducer task的并发任务数
 * @author Maxbin
 *
 */

public class FlowSumAreaPart {
	
	public static class AreaMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			String[] fields = StringUtils.split(line, "\t");
			
			String phoneNB = fields[0];
			
			long u_flow = Long.parseLong(fields[1]);
			long d_flow = Long.parseLong(fields[2]);
			
			context.write(new Text(phoneNB), new FlowBean(phoneNB,u_flow,d_flow));
			
		}
		
	}
	
	public static class AreaReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
		
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context)
				throws IOException, InterruptedException {
			
			long u_flow_counter = 0;
			long d_flow_counter = 0;
			
			for(FlowBean value:values) {
				
				u_flow_counter += value.getU_flow();
				d_flow_counter += value.getD_flow();
			}
			
			context.write(key, new FlowBean(key.toString(),u_flow_counter,d_flow_counter));

		}
			
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		//设置自定义的分组逻辑
		job.setPartitionerClass(AreaPartitioner.class);
		
		//根据分组个数来定义reducer task的个数
		job.setNumReduceTasks(6);
		
		job.setJarByClass(FlowSumAreaPart.class);
		
		job.setMapperClass(AreaMapper.class);
		job.setReducerClass(AreaReducer.class);
		
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
	
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
