package com.elon33.hadoop.hdfs;

// cc ShowFileStatusTest Demonstrates file status information
//import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.is;

import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;

// vv ShowFileStatusTest
public class ShowFileStatusTest {

	private MiniDFSCluster cluster; // use an in-process HDFS cluster for
									// testing
	private FileSystem fs;

	@Before
	public void setUp() throws IOException {
		//Configuration conf = new Configuration();
		if (System.getProperty("test.build.data") == null) {
			System.setProperty("test.build.data", "/tmp");
		}
		/*
		 * cluster = new MiniDFSCluster.Builder(conf).build(); fs =
		 * cluster.getFileSystem();
		 */
		// 定义hdfs集群的url：hdfs://hadoop:9000/ 定义conf对象
		// 根据uri和conf对象定义fs对象
		String uri = "hdfs://hadoop:9000/";
		Configuration conf = new Configuration();
		
		/*conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		 */
 		fs = FileSystem.get(URI.create(uri), conf);

		OutputStream out = fs.create(new Path("/dir/file"));
		out.write("content".getBytes("UTF-8"));
		out.close();
	}

	@After
	public void tearDown() throws IOException {
		if (fs != null) {
			fs.close();
		}
		if (cluster != null) {
			cluster.shutdown();
		}
	}

	@Test(expected = FileNotFoundException.class)
	public void throwsFileNotFoundForNonExistentFile() throws IOException {
		fs.getFileStatus(new Path("no-such-file"));
	}

	/*
	 * 文件状态列
	 */
	@Test
	public void fileStatusForFile() throws IOException {
		Path file = new Path("/dir/file");
		FileStatus stat = fs.getFileStatus(file);
		assertThat(stat.getPath().toUri().getPath(), is("/dir/file"));
		assertThat(stat.isDirectory(), is(false));
		assertThat(stat.getLen(), is(7L));
		// assertThat(stat.getModificationTime(),
		// is(lessThanOrEqualTo(System.currentTimeMillis())));
		assertThat(stat.getReplication(), is((short) 3));
		assertThat(stat.getBlockSize(), is(128 * 1024 * 1024L));
		assertThat(stat.getOwner(), is(System.getProperty("user.name")));
		assertThat(stat.getGroup(), is("supergroup"));
		assertThat(stat.getPermission().toString(), is("rw-r--r--"));
	}

	/*
	 * 文件目录状态列
	 */
	@Test
	public void fileStatusForDirectory() throws IOException {
		Path dir = new Path("/dir");
		FileStatus stat = fs.getFileStatus(dir);
		assertThat(stat.getPath().toUri().getPath(), is("/dir"));
		assertThat(stat.isDirectory(), is(true));
		assertThat(stat.getLen(), is(0L));
		// assertThat(stat.getModificationTime(),
		// is(lessThanOrEqualTo(System.currentTimeMillis())));
		assertThat(stat.getReplication(), is((short) 0));
		assertThat(stat.getBlockSize(), is(0L));
		assertThat(stat.getOwner(), is(System.getProperty("user.name")));
		assertThat(stat.getGroup(), is("supergroup"));
		assertThat(stat.getPermission().toString(), is("rwxr-xr-x"));
	}

}
// ^^ ShowFileStatusTest
