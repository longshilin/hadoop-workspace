package com.elon33.hadoop.hdfs;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListStatus {

	public static void main(String[] args) throws Exception {

		// 定义hdfs集群的url：hdfs://hadoop:9000/   定义conf对象  
		// 根据uri和conf对象定义fs对象
		String uri = "hdfs://hadoop:9000/";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);

		//获取参数列表，并将uri参数封装在Path[]数组中
		Path[] paths = new Path[args.length];
		for (int i = 0; i < paths.length; i++) {
			paths[i] = new Path(args[i]);
		}
		// 列出数组中各个URI目录下文件元数据对象
		FileStatus[] status = fs.listStatus(paths);
		// 遍历输出 文件元数据对象表示的目录下的内容
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for (Path p : listedPaths) {
			System.out.println(p);
		}
	}

}
