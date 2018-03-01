package com.maxbin.hadoop.flowsum;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * FlowBean是我们自定义的一种数据类型，要在hadoop的各个节点之间传输，应该遵循hadoop的序列化机制
 * 就必须实现hadoop相应的序列化接口
 * @author Maxbin
 *
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	//拿到日志中的一行数据，切分各个字段，抽取出我们需要的字段：手机号，上行流量，下行流量，然后封装成kv发送出去
	protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		
		//拿一行数据
		String line = value.toString();
		//切分各个字段
		String[] fileds = StringUtils.split(line, "\t");
		
		//拿到需要的字段
		String phoneNB = fileds[0];
		long u_flow = Long.parseLong(fileds[1]);
		long d_flow = Long.parseLong(fileds[2]);
		
		//封装数据为kv并输出
		context.write(new Text(phoneNB), new FlowBean(phoneNB,u_flow,d_flow));
		
	}
	
	

}
