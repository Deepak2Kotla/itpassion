package com.itpassion.deepak.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MostViewedVideos {

	public static class MyMapper extends Mapper <LongWritable,Text,Text,IntWritable>{
		private Text videoId = new Text();
    	private final static IntWritable views = new IntWritable(1);
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			String [] str = line.split("\\t");
			
			if (str.length > 7 ){
				videoId.set(str[0]);
				int viewCount = Integer.parseInt(str[5]);
				views.set(viewCount);
			}
			context.write(videoId, views);		
		}
		
	}
	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
           int sum = 0;
    	   int l = 0;
    		for ( IntWritable val : values){
    			l+= 1;
    			sum += val.get();
    		}
    		sum = sum/l;
    		context.write(key, new IntWritable(sum));
			
		}
	}
	public static void main(String[] args) throws Throwable {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"MostViewed");
		
		// Set Mapper and Reducer class
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJarByClass(MostViewedVideos.class);
		
		// Set Mapper Output Key and Value class 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// Set Reducer Output Key and Value class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Set InputFormat Class
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set Input and Out file paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// wait for job to completion
		System.exit(job.waitForCompletion(true) ? 0 :1);

	}

}
