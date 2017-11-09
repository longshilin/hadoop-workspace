package com.elon33.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsClientEasy {

	private FileSystem fs = null;

	@Before
	public void getFs() throws IOException {

		// get a configuration object
		Configuration conf = new Configuration();
		// to set a parameter, figure out the filesystem is hdfs
		conf.set("fs.defaultFS", "hdfs://hadoop:9000/");
		conf.set("dfs.replication", "1");

		// get a instance of HDFS FileSystem Client
		fs = FileSystem.get(conf);

	}

	/**
	 * 在hdfs文件系统中创建文件目录
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testMkdir() throws IllegalArgumentException, IOException {

		boolean res = fs.mkdirs(new Path("/upload"));
		System.out.println(res ? "mkdir is successfully!" : "it's failed!");

	}

	/**
	 * 给文件/文件夹重命名
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRename() throws IllegalArgumentException, IOException {

		boolean res = fs.rename(new Path("/upload"), new Path("/upload1"));
		System.out.println(res ? "mkdir is successfully!" : "it's failed!");
	}

	/**
	 * 上传文件到HDFS
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testUpload() throws IllegalArgumentException, IOException {

		fs.copyFromLocalFile(new Path("c:/input.txt"), new Path("/upload1"));

	}

	/**
	 * 从HDFS系统下载文件到本地
	 * 
	 * @throws Exception
	 */
	@Test
	public void testDownload() throws Exception {

		fs.copyToLocalFile(new Path("/jdk.exe"), new Path("f:/"));
	}

	/**
	 * 通过输入输出流完成上传文件 upload local file to HDFS
	 * 
	 * @throws Exception
	 */
	@Test
	public void testUploadByIO() throws Exception {

		// open a inputstream of the local source file
		FileInputStream is = new FileInputStream("c:/input.txt");

		// create a outputstream of the dest file
		Path destFile = new Path("hdfs://hadoop:9000/input.txt");
		FSDataOutputStream os = fs.create(destFile);

		// write the bytes in "is" to "os" ==> upload local file to HDFS
		// IOUtils.copy(is, os);

	}

	/**
	 * 通过输入输出流完成下载文件 download HDFS file to local
	 * 
	 * @throws Exception
	 */
	@Test
	public void testDownloadByIO() throws Exception {

		// open a FSDataInputStream of the HDFS source file
		FSDataInputStream is = fs.open(new Path("hdfs://hadoop:9000/input.txt"));
		// create a outputstream file of the local file
		FileOutputStream os = new FileOutputStream("f:/input.txt");
		// write the bytes in "is" to "os" ==> download HDFS file to local
		IOUtils.copy(is, os);
	}

	/**
	 * 删除hdfs文件系统中的文件
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRmfile() throws IllegalArgumentException, IOException {

		boolean res = fs.delete(new Path("/upload1"), true);
		System.out.println(res ? "delete is successfully :)" : "it is failed :(");

	}

	/**
	 * 查询出文件系统指定目录下的文件列表
	 * 
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {

		// List the statuses and block locations of the files in the given path
		// 循环遍历指定路径下的所有文件 同：hadoop fs -ls -R /
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

		while (listFiles.hasNext()) {

			LocatedFileStatus file = listFiles.next();

			System.out.println(file.getPath().getName());

		}

		System.out.println("--------------------------------------------");

		// List the statuses of the files/directories in the given path if the
		// path is a directory.
		// 列出该路径下的文件或者目录状态信息
		FileStatus[] status = fs.listStatus(new Path("/"));
		for (FileStatus file : status) {

			System.out.println(file.getPath().getName() + "   " + (file.isDirectory() ? "d" : "f"));
		}
	}
}
