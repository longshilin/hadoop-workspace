package com.elon33.hadoop.mapreduce.flowcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


//hadoop�Լ�ʵ�ֵ����л����Ƹ�jdk������ ��jdk������
/**
 * ʵ��
 * @author elon
 *
 */
public class FlowCount {

	
	public static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		private FlowBean flowBean = new FlowBean();
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			try {
				// �õ�һ������
				String line = value.toString();
				// �з��ֶ�
				String[] fields = StringUtils.split(line, "\t");
				// �õ�������Ҫ�����ɸ��ֶ�
				String phoneNbr = fields[1];
				long up_flow = Long.parseLong(fields[fields.length - 3]);
				long d_flow = Long.parseLong(fields[fields.length - 2]);
				// �����ݷ�װ��һ��flowbean��
				flowBean.set(phoneNbr, up_flow, d_flow);

				// ���ֻ���Ϊkey���������������ȥ��
				// ע�⣺�˴�ֻ���ֻ�����Ϊkey����ζ��ֻ���ֻ��Ų��������У�
				// ������������룬Ĭ�ϰ������ֵ�����
				context.write(new Text(phoneNbr), flowBean);
			}catch(Exception e){
				System.out.println("exception occured in mapper" );
			}
			
		}
	}
	
	
	public static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		private FlowBean flowBean = new FlowBean();
		
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values,Context context)
				throws IOException, InterruptedException {

			long up_flow_sum = 0;
			long d_flow_sum = 0;
			
			for(FlowBean bean:values){
				
				up_flow_sum += bean.getUp_flow();
				d_flow_sum += bean.getD_flow();
				
			}
			
			flowBean.set(key.toString(), up_flow_sum, d_flow_sum);
			
			context.write(new Text(flowBean.getPhoneNbr()), flowBean);
			
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"flowjob");
		
		job.setJarByClass(FlowCount.class);
		
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("c:/wc1/srcdata/HTTP_20130313143750.dat"));
		FileOutputFormat.setOutputPath(job, new Path("c:/wc1/output"));
		
		job.waitForCompletion(true);
		
		
	}
	
}