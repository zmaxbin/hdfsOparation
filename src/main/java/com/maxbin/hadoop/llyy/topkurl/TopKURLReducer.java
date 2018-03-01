package com.maxbin.hadoop.llyy.topkurl;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.maxbin.hadoop.flowsum.FlowBean;

public class TopKURLReducer extends Reducer<Text, FlowBean, Text, LongWritable>{
	
	private TreeMap<FlowBean, Text> treeMap = new TreeMap<FlowBean, Text>();
	private double globalCount = 0;
	
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		
		Text url = new Text(key.toString());
		long up_sum = 0;
		long d_sum = 0;
		
		for(FlowBean bean : values) {
			
			up_sum += bean.getU_flow();
			d_sum += bean.getD_flow();
		}
		
		FlowBean bean = new FlowBean("", up_sum, d_sum);
		
		globalCount += bean.getS_flow();
		
		treeMap.put(bean, url);
		
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		Set<Entry<FlowBean, Text>> entrySet = treeMap.entrySet();
		
		double tempCount = 0;
		
		for(Entry<FlowBean, Text> ent: entrySet) {
			
			if(tempCount / globalCount < 0.8) {
			
				context.write(ent.getValue(), new LongWritable(ent.getKey().getS_flow()));
				
				tempCount += ent.getKey().getS_flow();
				
			} else {
				return;
			}
			
		}
		
		
		
		
	}

}
