package kmeans;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Scanner; 
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Kmeans {

  public static class dMapper
       extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable>{
 //private final static IntWritable temp = new IntWritable();
 //private final static IntWritable j = new IntWritable();
    
    @Override
    public void map(Object key, Object value, Context context
                    ) throws IOException, InterruptedException {
    //code
	//should implement euclidean dist
	//and return <j,dij>
	HashMap<Double,Double> temp=new HashMap<Double,Double>();
	temp.put(i,dij);
	context.write(j,temp);
	}
  }

   public static class dReducer
       extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    //private final static IntWritable i = new IntWritable();
   
    public void dreduce(Object key, Iterable<HashMap> values,
                       Context context
                       ) throws IOException, InterruptedException {
	//code				
//find the min distance and return <j,i>	
	min=0;
	i=0;
	for(HashMap temp: values){
		if(temp[1]<=min){
			min=temp[1];
			i=temp[0];
		}
	}
	context.write(key,i);
    }
  }
  
   public static class cMapper
       extends Mapper<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable>{

    
    public void cmap(Object key, Object value, Context context
                    ) throws IOException, InterruptedException {
    //code
	//input is <j,i> from dReducer, exchange to <i,j>change of centroids 
	}
  }

   public static class cReducer
       extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    
    public void creduce(Object key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    //code
	// calc. new centroids
	}
  }
 
  //here we need to load the dataset into a hashmap and 
  //
   public static void main(String[] args) throws Exception {
    //code
	HashMap<Integer, Array> data = new HashMap<Integer, Array>();
	   for(int i=0; i<= 10000; i++)
	   {
  // Open the file
  FileReader fr = new FileReader("mnist_data.txt"); 
	   BufferedReader br = new BufferedReader(fr); 
	   
  String strLine;
  //Read File Line By Line
  while ((strLine = br.readLine()) != null)   {
  // split the line on your splitter(s)
     String[] splitted = strLine.split(" "); // 
  }
  //Close the input file
  fr.close();
     StringTokenizer strtokenizer = new StringTokenizer(strLine);
	 // loop in order to take all tokens

        while(strtokenizer.hasMoreTokens()){
			 String token = strLine.nextToken();
		int something = Integer.parseInt(token);
        
		data.put(i,array);
        }
		//then initialize centroids again with a hashmap
		HashMap prev=null;
		HashMap<Integer,Array> centroids=new HashMap<Integer,Array>();
		//to fill

		//create mapreduce jobs and chain them
	Configuration conf = getConf();
  FileSystem fs = FileSystem.get(conf);
  Job job = new Job(conf, "dJob");
  job.setJarByClass(Kmeans.class);

  job.setMapperClass(dMapper.class);
  job.setReducerClass(dReducer.class);

  //job.setOutputKeyClass(Text.class);
  //job.setOutputValueClass(IntWritable.class);

  //job.setInputFormatClass(TextInputFormat.class);
  //job.setOutputFormatClass(TextOutputFormat.class);

  FileInputFormat.addInputPath(job, new Path(args[2]));
  FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
  
  //job.waitForCompletion(true);

  /*
   * Job 2
   */
  
  Job job2 = new Job(conf, "cJob");
  job2.setJarByClass(Kmeans.class);

  job2.setMapperClass(cMapper.class);
  job2.setReducerClass(cReducer.class);

  //job2.setOutputKeyClass(Text.class);
  //job2.setOutputValueClass(Text.class);

  //job2.setInputFormatClass(TextInputFormat.class);
  //job2.setOutputFormatClass(TextOutputFormat.class);

  FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
  FileOutputFormat.setOutputPath(job2, new Path(args[1]));
  job2.addDependingJob(job);
  //return job2.waitForCompletion(true) ? 0 : 1;
 
 if (args.length != 2) {
   System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
   System.exit(0);
  }
  ToolRunner.run(new Configuration(), new ChainJobs(), args);
 
	//finally implement convergence condition and create output directory to load the results
	//remember the inputs should be passed as args
		
	
	}
   }
}
