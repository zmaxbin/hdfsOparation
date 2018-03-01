package com.maxbin.hadoop.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
	
	//框架每次传递一组数据<138793922987,{flowbean,flowbean,flowbean,...}>调用一次reducer方法
	protected void reduce(Text key,Iterable<FlowBean> values,Context context) throws IOException, InterruptedException {
		
		long u_flow_counter = 0;
		long d_flow_counter = 0;
		
		for(FlowBean bean : values) {
			u_flow_counter += bean.getU_flow();
			d_flow_counter += bean.getD_flow();
		}
		
		context.write(key, new FlowBean(key.toString(),u_flow_counter,d_flow_counter));
		
	}

}
