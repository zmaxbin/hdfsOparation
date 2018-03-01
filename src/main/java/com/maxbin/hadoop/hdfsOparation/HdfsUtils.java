package com.maxbin.hadoop.hdfsOparation;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsUtils {
	
	FileSystem fs = null;
	
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()); 
		conf.set("fs.defaultFS","hdfs://172.16.56.100:9000/");
		
		fs = FileSystem.get(new URI("hdfs://172.16.56.100:9000/"), conf, "hadoop");
	}
	
	/**
	 * 上传文件
	 * @throws IOException 
	 * @throws URISyntaxException 
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("resource")
	@Test
	public void upload() throws IOException, InterruptedException, URISyntaxException {
		
//		Configuration conf = new Configuration();
//		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()); 
//		conf.set("fs.defaultFS","hdfs://172.16.56.100:9000/");
//		FileSystem fs = FileSystem.get(new URI("hdfs://172.16.56.100:9000/"), conf, "hadoop");
		
		Path dst = new Path("hdfs://172.16.56.100:9000/test3.txt");
		
		FSDataOutputStream os = fs.create(dst);
		
		FileInputStream is = new FileInputStream("E:/test3.txt");
		
		
		IOUtils.copy(is, os);
		
	}
	
	@Test
	public void upload2() throws IllegalArgumentException, IOException {
		
		fs.copyFromLocalFile(new Path("D:/test2.txt"), new Path("hdfs://192.168.159.100:9000/test2.txt"));
	}
	
	/**
	 * 下载文件
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void download() throws IllegalArgumentException, IOException {
		
		fs.copyToLocalFile(new Path("hdfs://192.168.159.100:9000/test2.txt"), new Path("D:/test3.txt"));
		
	}
	
	/**
	 * 查看文件信息
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	 */
	@Test
	public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
		
		while(files.hasNext()) {
			
			LocatedFileStatus file = files.next();
			Path filePath = file.getPath();
			String fileName = filePath.getName();
			System.out.println(fileName);
			
			
		}
		
	}
	
	/**
	 * 创建目录
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void mkdir() throws IllegalArgumentException, IOException {
		
		fs.mkdirs(new Path("/aaa/bbb"));
		
	}
	
	/**
	 * 删除文件或者文件夹
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void rm() throws IllegalArgumentException, IOException {
		
		fs.delete(new Path("/aaa"), true);
		
	}
	

}
