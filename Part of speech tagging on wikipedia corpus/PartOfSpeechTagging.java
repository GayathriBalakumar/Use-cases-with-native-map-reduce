import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class PartOfSpeechTagging{

	
   public static class POSMapper extends Mapper<Object, Text, IntWritable, Text >{

   private final static IntWritable wordlength = new IntWritable();
   private Text word = new Text();
   private  Map<String, String> posMap  = new HashMap<String, String>();;
   
   
   
   public void setup(Context context) {
   	Configuration conf = context.getConfiguration();
   	Path mposfilePath = new Path("hdfs://cshadoop1/user/gxb170030/assignment1b/mobyposi_text.txt");
   	BufferedReader br;
   	FileSystem fs;
	try {
		fs = FileSystem.get(conf);
		br=new BufferedReader(new InputStreamReader(fs.open(mposfilePath)));
	   	String line, word, type;
	   	  while ((line=br.readLine())!= null){
	   		  word = line.substring(0,line.indexOf("*"));
	   		  type = line.substring(line.indexOf("*")+1);
	   		  for(int i=0;i<type.length();i++)		 
	   			posMap.put(word.toLowerCase(), type);
	   		  
	   	  }
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   
   	}
   

   public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
	   StringTokenizer itr = new StringTokenizer(value.toString());
	      String token;
	      String palinFlag="0";
	      try {
			 while (itr.hasMoreTokens()) {
				 token = itr.nextToken().trim().toLowerCase();   
				 if(token.length() >=5 && posMap.containsKey(token)){	
					 if (token.equals(new StringBuilder(token).reverse().toString()))
						palinFlag="1";
					 word.set(posMap.get(token)+palinFlag);
					 wordlength.set(token.length());
					 context.write(wordlength, word);
					}
			  }
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			}     
	   	
   		}
	  } 

 public static class SummaryReducer extends Reducer<IntWritable,Text,Text,Text> {
	  
	
	 
	 public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		 int wordLength = key.get();
		 int wordCount =0 , palinCount =0 , NCount =0, pCount =0 , hCount =0 , VCount =0 , tCount =0 , iCount =0 , ACount =0 ;
		 int vCount =0 , CCount =0 , PCount =0 , itCount =0 , rCount =0 , DCount =0 , ICount =0 , oCount =0;
     for (Text val : values) {
    	 
    	 wordCount++;
    	 if(val.getLength() > 1)
    	 {
    	 String pos= val.toString().substring(0,1);
    	 String palinFlag = val.toString().substring(1);
    	 if(palinFlag.equals("1"))
    		 palinCount++;
    	 
    	 if(pos.equals("N"))
			NCount++;
    	 else if(pos.equals("p"))
    		pCount++;
    	else if(pos.equals("h"))
 			hCount++;				
    	else if(pos.equals("V"))	
    		VCount++;
    	else if(pos.equals("t"))
				tCount++;
    	else if(pos.equals("i"))
				iCount++;
    	else if(pos.equals("A"))
				ACount++;
    	else if(pos.equals("v"))
				vCount++;
    	else if(pos.equals("C"))
				CCount++;
    	else if(pos.equals("p"))
				pCount++;
    	else if(pos.equals("!"))
				itCount++;
    	else if(pos.equals("r"))
				rCount++;
    	else if(pos.equals("D"))
				DCount++;
    	else if(pos.equals("I"))
				ICount++;
    	else if(pos.equals("o"))
		        oCount++;
			
    	
    	 }
     }
     
	 context.write(new Text("Length:"), new Text(Integer.toString(wordLength)));
	 context.write(new Text("Count of Words :"), new Text(Integer.toString(wordCount)));
	 context.write(new Text("Distribution of POS : "), new Text("{ noun: "+NCount+" plural: "+pCount+" Noun Phrase: "+hCount+" Participle Verb: "+VCount+" Transitive Verb: "+ tCount +" Intransitive Verb: "+ iCount +" Adjective: "+ ACount+" Adverb: "+ vCount+ " Conjunction : "+CCount+" Preposition: "+PCount +" Interjection: "+itCount+" Pronoun: "+rCount+" Definitie Article: "+DCount+" Indefinitie Article: "+ICount+" Nominative: "+oCount  +"}"));
	 context.write(new Text("Number of palindromeindromes :"), new Text(Integer.toString(palinCount)));
  
             }
   
 }
   
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
	    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
	    conf.set("mapreduce.framework.name", "yarn");
	    Job job = Job.getInstance(conf, "part 2");
	    job.setJarByClass(PartOfSpeechTagging.class);
	    job.setMapperClass(POSMapper.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    //job.setCombinerClass(SummaryReducer.class);
	    job.setReducerClass(SummaryReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(1);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	
}










