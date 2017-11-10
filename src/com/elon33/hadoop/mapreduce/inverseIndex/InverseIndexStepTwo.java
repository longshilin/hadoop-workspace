package com.elon33.hadoop.mapreduce.inverseIndex;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 倒排索引的第二个步骤、
 * 将不同文件中的同一个word，统计”文件-->计数“的信息，并拼接在一起输出
 * @author elon@elon33.com
 *
 */
public class InverseIndexStepTwo {

	public static class InverseIndexStepTwoMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			// 切分出各个字段
			String[] fields = StringUtils.split(line, "\t");
			long count = Long.parseLong(fields[1]);
			String wordAndFile = fields[0];
			String[] wordAndFileName = StringUtils.split(wordAndFile, "-->");
			String word = wordAndFileName[0];
			String fileName = wordAndFileName[1];

			//将单词作为key  ，文件-->次数 作为value 输出
			k.set(word);
			v.set(fileName + "-->" + count);
			context.write(k, v);
		}

	}

	
	public static class InverseIndexStepTwoReducer extends Reducer<Text, Text, Text, Text>{
		
//		private Text k = new Text();
		private Text v = new Text();
		
		// key: hello    values: [a-->3,b-->2,c-->1]
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {

			String result = "";
			
			//一个key，有多个value 用result将多个value拼接在一起
			for(Text value:values){
				result += value + " ";
			}
			v.set(result);
			// key: hello    v:  a-->3 b-->2 c-->1 
			context.write(key, v);
			
		}
		
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job_stepTwo = Job.getInstance(conf);
		
		job_stepTwo.setJarByClass(InverseIndexStepTwo.class);
		
		job_stepTwo.setMapperClass(InverseIndexStepTwoMapper.class);
		job_stepTwo.setReducerClass(InverseIndexStepTwoReducer.class);
		
		job_stepTwo.setOutputKeyClass(Text.class);
		job_stepTwo.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job_stepTwo, new Path("./wc/outdata/output5/part-r-00000"));
		FileOutputFormat.setOutputPath(job_stepTwo, new Path("./wc/outdata/output6"));
		
		job_stepTwo.waitForCompletion(true);
		
	}
}
