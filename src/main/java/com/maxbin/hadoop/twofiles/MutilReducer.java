package com.maxbin.hadoop.twofiles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MutilReducer extends Reducer<Text, FlagString, Text, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<FlagString> values,
			Context context) throws IOException, InterruptedException {
		
		String name = "";
		
		ArrayList<String> urlList = new ArrayList<String>();
		
		//urlList不能存放value这个实例，因为value是迭代出来的，所以会指向同一个
		
		for (FlagString value : values) {
			if (value.getFlag() == 0) {
				name = value.getValue();
			} else {
				urlList.add(value.getValue());
			}
			
		}
		
//		for (String url : urlList) {
//			context.write(new Text(key.toString()), new Text(name + "\t" + url));
//		}
		@SuppressWarnings({ "rawtypes", "unchecked" })
		HashSet hashSet = new HashSet(urlList);
		for (Object temp : hashSet) {
			int frequency = Collections.frequency(urlList, temp);
			context.write(new Text(key.toString()), new Text(name + "\t" + temp.toString() + "\t" + String.valueOf(frequency)));
		}
		
	}

}
