import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class PolarityClassification{

	
	 public static class WordPolaritizerMapper
      extends Mapper<Object, Text, Text, IntWritable>{

   private final static IntWritable one = new IntWritable(1);
   private Text positive = new Text("positive");
   private Text negative = new Text("negative");

   public void map(Object key, Text value, Context context
                   ) throws IOException, InterruptedException {
	   
	   Path positivefilePath = new Path("hdfs://cshadoop1/user/gxb170030/assignment1b/positive-words.txt");
	   Path negativefilePath = new Path("hdfs://cshadoop1/user/gxb170030/assignment1b/negative-words.txt");
	   FileSystem fs = FileSystem.get(new Configuration());
	   BufferedReader br1=new BufferedReader(new InputStreamReader(fs.open(positivefilePath)));
	   BufferedReader br2=new BufferedReader(new InputStreamReader(fs.open(negativefilePath)));
	   StringTokenizer itr = new StringTokenizer(value.toString());
	   
	   
	   while (itr.hasMoreTokens()) {
       String word = itr.nextToken(); 
       String line;
       			while( (line = br1.readLine() ) != null)
       				{
       					if(line.equalsIgnoreCase(word))	  
       					context.write(positive, one);
       				}
       			while( (line = br2.readLine() ) != null)
       				{
       					if(line.equalsIgnoreCase(word))	  
       					context.write(negative, one);
       				} 		
	   }
   		}
	  } 

 public static class TotalCounterReducer
      extends Reducer<Text,IntWritable,Text,IntWritable> {
	 private IntWritable result = new IntWritable();

	 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
     int sum = 0;
     for (IntWritable val : values) {
       sum += val.get();
     }
     result.set(sum);
     context.write(key, result);
     
     
   }
   
 }
   
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
	    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
	    conf.set("mapreduce.framework.name", "yarn");
	    Job job = Job.getInstance(conf, "part 1");
	    job.setJarByClass(part1.class);
	    job.setMapperClass(WordPolaritizerMapper.class);
	    job.setCombinerClass(TotalCounterReducer.class);
	    job.setReducerClass(TotalCounterReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setNumReduceTasks(2);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	
}
