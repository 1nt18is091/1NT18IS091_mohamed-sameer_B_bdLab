package my.pack.ex1;
// Total number of awards for those with salary > 30000

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class Prog3 {

	//MAPPER CODE	
		   
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	
	// the variable one holds the value 1 in the form of Hadoop's IntWritable Object
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String myvalue = value.toString();
		String[] tokens = myvalue.split(",");
		Text award = new Text("Awards for over 30k: ");
		if (Integer.parseInt(tokens[2]) > 30000) {
			output.collect(award, new IntWritable(Integer.parseInt(tokens[3])));
		}
		}
	}

	//REDUCER CODE	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
			int transcount = 0;
			while(values.hasNext()) {
				transcount += values.next().get();
			}
			
			output.collect(new Text("Awards for over 30k: "), new IntWritable(transcount));
		}
	}
		
	//DRIVER CODE
	public static void main(String[] args) throws Exception {
	
		JobConf conf = new JobConf(Prog3.class);
		conf.setJobName("Various Operations");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class); 
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);   
	}
}