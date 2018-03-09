package com.maxbin.hadoop.InverseIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedMR {
	
	public static class InvertedMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String lines = value.toString();
			
			String[] fields = StringUtils.split(lines, " ");
			
			//由于context.getInputSplit()返回一个抽象类InputSplit，所以需要用(Ctrl+T)找到具体实现类
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			//获取split的所在的文件名
			String fileName = inputSplit.getPath().getName();
			
			for (String field : fields) {
				context.write(new Text(field), new Text(fileName));
			}
		}
	}
	
	public static class InvertedReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			ArrayList<String> fileList = new ArrayList<String>();
			for (Text text: values) {
				fileList.add(text.toString());
			}
			
			Map<String, Integer> hashMap = new HashMap<String, Integer>();
			for (String text: fileList) {
			    if (hashMap.get(text) == null) {
			    	hashMap.put(text, 1);
			    } else {
			    	hashMap.put(text, hashMap.get(text) + 1);
			    }
			}
			
			Set<String> set=hashMap.keySet();
			Iterator<String> iter=set.iterator();
			String fullInfo = "";
			while (iter.hasNext()) {
			    Object myKey=iter.next();
			    Object value=hashMap.get(myKey);
			    fullInfo += myKey + "-->" + value.toString() + " ";
//			    System.out.println(key);
//			    System.out.println(myKey + "-->" + value.toString());
			}
			System.out.println(fullInfo.trim());
			context.write(key, new Text(fullInfo.trim()));
		}
	}

}
