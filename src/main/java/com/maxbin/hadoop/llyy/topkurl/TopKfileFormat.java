package com.maxbin.hadoop.llyy.topkurl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKfileFormat<K, V> extends FileOutputFormat<K, V>{
	
	//RecordWriter<K, V>是一个抽象类，所以需要写一个静态类来作为子类
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataOutputStream enhancedOs = fs.create(new Path("/liuliang/output/enhanced"));
		FSDataOutputStream tocrawlOs = fs.create(new Path("/liuliang/output/tocrawl"));
		
		
		return new TopKgetRecordWriter<K, V>(enhancedOs, tocrawlOs);
	}
	
	public static class TopKgetRecordWriter<K, V> extends RecordWriter<K, V> {
		
		private FSDataOutputStream enhancedOs = null;
		private FSDataOutputStream tocrawlOs = null;
		
		public TopKgetRecordWriter(FSDataOutputStream enhancedOs, FSDataOutputStream tocrawlOs) {
			
			this.enhancedOs = enhancedOs;
			this.tocrawlOs = tocrawlOs;
			
		}

		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			
			if(key.toString().endsWith("tocrawl")) {	
				tocrawlOs.write(key.toString().getBytes());
			} else {
				enhancedOs.write(key.toString().getBytes());
			}
			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			
			if(enhancedOs != null) {
				enhancedOs.close();
			}
			if(tocrawlOs != null) {
				tocrawlOs.close();
			}
			
		}
		
	}

}
