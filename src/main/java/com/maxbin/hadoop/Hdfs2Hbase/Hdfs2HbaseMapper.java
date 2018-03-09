package com.maxbin.hadoop.Hdfs2Hbase;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Hdfs2HbaseMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String lineStr = value.toString();
		int index = lineStr.indexOf(",");
		String rowKey = lineStr.substring(0, index);
		String left = lineStr.substring(index+1);
		context.write(new Text(rowKey), new Text(left));
	}
	


}
