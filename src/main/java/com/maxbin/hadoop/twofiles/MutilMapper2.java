package com.maxbin.hadoop.twofiles;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MutilMapper2 extends Mapper<LongWritable, Text, Text, FlagString>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		String[] fileds = StringUtils.split(line, "\t");
		
		String id = fileds[0];
		String url = fileds[1];
		
		context.write(new Text(id), new FlagString(url, 1));
	}

}

