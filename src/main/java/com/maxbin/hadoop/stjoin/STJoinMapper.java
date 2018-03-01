package com.maxbin.hadoop.stjoin;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class STJoinMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		String [] fields = StringUtils.split(line, "\t");
		
		String child = fields[0];
		String parent = fields[1];
		
        context.write(new Text(child), new Text("-" + parent));
        context.write(new Text(parent), new Text("+" + child)); 
	}

}
