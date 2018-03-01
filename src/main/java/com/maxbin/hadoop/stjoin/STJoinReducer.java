package com.maxbin.hadoop.stjoin;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class STJoinReducer extends Reducer<Text, Text, Text, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// map过来的数据为 <Jack,{-Alice,-Jesse,+Tom,+Jone}>
		// <Tom,{-Jack,-Lucy}
		
        ArrayList<Text> grandparent = new ArrayList<Text>();
        ArrayList<Text> grandchild = new ArrayList<Text>();
		
		for (Text value : values) {
			String s = value.toString();
			if (s.startsWith("-")) {
				grandparent.add(new Text(s.substring(1)));
			} else {
				grandchild.add(new Text(s.substring(1)));
			}
		}
		
		for (Text text1 : grandchild) {
			for (Text text2 : grandparent) {
				context.write(text1, text2);
			}
		}
		
	}

}
