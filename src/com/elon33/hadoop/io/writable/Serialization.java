package com.elon33.hadoop.io.writable;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.is;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class Serialization {

	IntWritable writable = new IntWritable(163);

	/*
	 * @Before public void setUp() throws IOException { writable = new
	 * IntWritable(); writable.set(163); }
	 */

	/*
	 * 序列化机制
	 */
	public static byte[] serialize(Writable writable) throws IOException {
		// 加入一个帮助函数封装字节流，以便在序列化流中捕捉字节
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataout = new DataOutputStream(out);
		writable.write(dataout);	// 将状态写入DataOutput二进制流
		dataout.close();
		return out.toByteArray();
	}

	/*
	 * 反序列化机制
	 */
	public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);	// 从DataInput二进制流读取状态到writable接口中
		dataIn.close();
		return bytes;
	}

	/*
	 * 序列化和反序列化测试类
	 */
	@Test
	public void TestSerialization() throws IOException {
		byte[] bytes = serialize(writable);
		assertThat(bytes.length, is(4));

		IntWritable newWritable = new IntWritable();
		deserialize(newWritable, bytes);
		assertThat(newWritable.get(), is(163));
	}

}
