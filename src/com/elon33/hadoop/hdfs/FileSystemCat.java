package com.elon33.hadoop.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 将文件系统中的一个文件在标准输出上显示
 * @author elon
 *
 */
public class FileSystemCat {

	public static void main(String[] args) throws Exception {

		String uri = "hdfs://hadoop:9000/wc/srcdata/input.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			in.seek(0);
			System.out.println("");
			System.out.println("********************");
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}
		
		
		
	}

}
