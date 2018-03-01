package com.maxbin.hadoop.llyy.topkurl;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.maxbin.hadoop.flowsum.FlowBean;

public class TopKURLMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	private FlowBean bean = new FlowBean();
	private Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		String[] fields = StringUtils.split(line, "\t");
		
		try {
			if(fields.length > 33 && StringUtils.isNotEmpty(fields[26]) && fields[26].startsWith("http")) {

				String url = fields[26];
				
				long up_flow = Long.parseLong(fields[30]);
				long d_flow = Long.parseLong(fields[31]);
				
				k.set(url);
				bean.set("", up_flow, d_flow);
				
				context.write(k, bean);
			}
		
		} catch(Exception e) {
			System.out.println("Exception occured in mapper...");
		}
		
		
	}

}
