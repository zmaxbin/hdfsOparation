package com.maxbin.hadoop.Hdfs2Hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Hdfs2HbaseReducer extends Reducer<Text, Text, ImmutableBytesWritable, Put>{
	
	@Override
	protected void reduce(Text rowKey, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		
		String k = rowKey.toString();
		for(Text val : value) {
			Put put = new Put(k.getBytes());
			String[] strs = val.toString().split(",");
//			put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(strs[0]));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(strs[0]));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(strs[1]));
			context.write(new ImmutableBytesWritable(k.getBytes()), put);
			
		}
	}

}
