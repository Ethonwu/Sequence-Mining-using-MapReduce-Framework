import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.StringTokenizer;
import java.lang.Math;
import java.math.BigInteger;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SequenceM {
public static class CountingMapper 
    extends Mapper<Object, Text, Text, IntWritable>{
 private String T = new String();
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
   while (itr.hasMoreTokens()) {
	 T = itr.nextToken().toString();
	 context.write(new Text(T), new IntWritable(1));
	 }
    
   }
 }

public static class LinesSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		 

		 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		   int sum = 0;
		   for (IntWritable val : values) {
		     sum += val.get();
		   }
		  
		 
		   context.write(key,new IntWritable(sum));
		 
		 }
		}
	
public static class TokenizerMapper 
    extends Mapper<Object, Text, Text, IntWritable>{
	public ArrayList<String> Compare = new ArrayList<String>();
	public ArrayList<String> val = new ArrayList<String>();
//	private Set<String> stopWordList = new HashSet<String>();
	private BufferedReader fis;
	public void setup(Context context) throws IOException{
		 Configuration conf = context.getConfiguration();
		 FileSystem fs = FileSystem.get(conf);
		 URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
		 Path getPath = new Path(cacheFiles[0].getPath());  
		 //System.out.println("De:"+getPath.toString());
		 BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(getPath)));
		 String setupData = null;
		  Compare.clear();
		 while ((setupData = bf.readLine()) != null) {
			 //System.out.println("Here:"+setupData.split("\t")[1]);
			 //stopWordList.add(setupData);
			 String t = setupData.split("\t")[0]+","+setupData.split("\t")[1];
			// System.out.println("Here is:"+t);
			 Compare.add(t);
		
			 }
		}
 private String T = new String();
 private String SubT = new String();
 private IntWritable one = new IntWritable(1);
 private Text word = new Text();
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   if(!value.toString().equals(""))
   {
	   
	   StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
	   
	   while (itr.hasMoreTokens()) {
	 
		 T = itr.nextToken();
		 int[] fk_num = new int[T.split(",")[0].length()];
		 int index = 0;
		 StringTokenizer itr_split = new StringTokenizer(T.split(",")[0], " ");
		 while(itr_split.hasMoreTokens())
		 {
			 fk_num[index]= Integer.valueOf(itr_split.nextToken());
			 index++;
		 }
		 for(String t:Compare) {
			 
			 String[] t_sp = t.split(",")[0].split(" ");
			 int[] list = new int[t_sp.length];
			 for(int i=0;i<t_sp.length;i++) {
				 list[i] = Integer.parseInt(t_sp[i]);
			 }
			 int k = t_sp.length;
			 int flag=0;
			 String prune_T = "";
			 for(int i=0;i<k;i++)
			 {
				 for(int j=0;j<index;j++)
				 {
					 if(list[i]==fk_num[j])
					 {
						 if(flag==0)
							 prune_T = Integer.toString(list[i]);
						 else
							 prune_T = prune_T + " " + Integer.toString(list[i]);
						 flag++;
						 
					 }
				 }
			 }
			 if(flag==index)
			 {
				 context.write(new Text(prune_T), new IntWritable(Integer.parseInt(t.split(",")[1])));
			 }
		 }
		
		 
		 
		 //context.write(new Text(temp), new IntWritable(1));
		
		 
		 
	  }
   }
   else 
	   System.out.println("Fuck");
  
	
   
 }
}
public static class IntSumReducer 

    extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result = new IntWritable();

 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
   int sum = 0;
   Configuration conf = context.getConfiguration();
	 String temp = conf.get("Support");
	 int support = Integer.parseInt(temp);
   for (IntWritable val : values) {
	   sum += val.get();
	// context.write(key, null);
   }
   if(sum>=support) {
     result.set(sum);
     context.write(key, result);
   }
 }
}
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 6) {
      System.err.println("Usage: wordcount <input><output><Fk path><minsup><Transactions><<Maps>");
      System.exit(2);
    }
    //Setting Timer
    Date date; 
	long start, end; 
	date = new Date(); start = date.getTime(); 
	//Setting Timer
    
    //Count Min_sup
    double Min_sup = Double.parseDouble(args[3]);
    int Transactions = Integer.parseInt(args[4].toString());
	System.out.println("Transactions number are: "+Transactions);
	double Support = (float)Transactions*Min_sup;
	int supp = (int)Support;
	conf.set("Support", Integer.toString(supp));
   // Count Min_sup
	
    //Setting Maps
	int Tasks = Integer.parseInt(args[5].toString());
    //Setting Maps
	
   // Input is Fk
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    outputPath.getFileSystem(conf).delete(outputPath, true);
    Path FkPath = new Path(args[2]);
      
      Job job = new Job(conf, "Fit T_Dataset");
	    job.setJarByClass(SequenceM.class); 
	    Path tempPath = new Path("/temp");
	    tempPath.getFileSystem(conf).delete(tempPath, true);
	    job.setMapperClass(CountingMapper.class);
	    job.setReducerClass(LinesSumReducer.class);	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setNumReduceTasks(1);
	    FileInputFormat.addInputPath(job, inputPath);
	    FileOutputFormat.setOutputPath(job, tempPath);
	    job.waitForCompletion(true);
      
	//Add T_Dataset to DisCache   
	    Path temp = new Path(tempPath+"/part-r-00000");
	    DistributedCache.addCacheFile(temp.toUri(), conf);
    // Add T_Dataset to DisCache  
	   
	    
    	    Job job2 = new Job(conf, "Seqence Mining");
    	    job2.setJarByClass(SequenceM.class); 
    	    job2.setMapperClass(TokenizerMapper.class);
    	    job2.setReducerClass(IntSumReducer.class);
    	    
    	    job2.setOutputKeyClass(Text.class);
    	    job2.setOutputValueClass(IntWritable.class);
    	    job2.setNumReduceTasks(Tasks);
    	    FileInputFormat.addInputPath(job2, FkPath);
    	    FileOutputFormat.setOutputPath(job2, outputPath);
    	    job2.waitForCompletion(true);
    	   
   
    	    date = new Date(); end = date.getTime();   
    		System.out.printf("Run Time is:%f",(end-start)*0.001F);
    		System.out.println("Finish!!");
    		System.exit(0);
}
}
