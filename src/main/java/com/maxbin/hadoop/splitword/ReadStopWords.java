package com.maxbin.hadoop.splitword;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadStopWords {
	
	public static Set<String> readWordFile() throws Exception{
			
		Set<String> wordSet = null;
		
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		
		Path dst = new Path("/cutword/input/stopWord.txt");
		
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(dst)));
		
		wordSet = new HashSet<String>();
		
		try {
			  String line;
			  line=br.readLine();
			  while (line != null){
			    line = br.readLine();
			    wordSet.add(line);
			  }
			} finally {
			  br.close();
			}
			
		return wordSet;
	}
			

}
