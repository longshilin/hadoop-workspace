package com.elon33.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountRunner {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job wcjob = Job.getInstance();

		// 设置job所使用的jar包
		conf.set("mapreduce.job.jar", "wcount.jar");

		// 设置wcjob中的资源所在的jar包
		wcjob.setJarByClass(WordCountRunner.class);

		// wcjob要使用哪个mapper类
		wcjob.setMapperClass(WordCountMapper.class);
		// wcjob要使用哪个reducer类
		wcjob.setReducerClass(WordCountReducer.class);

		// wcjob的mapper类输出的kv数据类型
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(LongWritable.class);

		// wcjob的reducer类输出的kv数据类型
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(LongWritable.class);

		// 指定要处理的原始数据所存放的路径
		FileInputFormat.setInputPaths(wcjob, "./wc/srcdata/input1");

		// 指定处理之后的结果输出到哪个路径
		FileOutputFormat.setOutputPath(wcjob, new Path("./wc/outdata/output1"));

		// Submit the job to the cluster and wait for it to finish.
		boolean res = wcjob.waitForCompletion(true);

		System.exit(res ? 0 : 1);
	}

}
