import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Kmeans extends Configured implements Tool  {
	static HashMap<Integer, Double[]> data;
	static HashMap<Integer, Double[]> prev;
	static HashMap<Integer, Double[]> centroids;
	static int k, n;
	private static FileReader fileReader;
	private static BufferedReader bufferedReader;
	private FileReader outFileReader;
	private BufferedReader outBufferedReader;

	public static class dMapper
	extends Mapper<LongWritable, IntWritable, LongWritable, ArrayWritable>{

		public void map(LongWritable key, IntWritable value, Context context
				) throws IOException, InterruptedException {
			for (int i = 0; i< k; i++){
				Double sum = 0.0;
				for (int j = 0; j< n; j++){
					sum = sum + Math.pow(data.get(key)[j] - centroids.get(i)[j], 2);
				}
				ArrayWritable outValueWritable = new ArrayWritable (DoubleWritable.class);
				DoubleWritable[] outValue = new DoubleWritable[2];
				outValue[0] = new DoubleWritable((new Double(i)).doubleValue());
				outValue[1] = new DoubleWritable(Math.sqrt(sum));
				outValueWritable.set(outValue);
				context.write(key, outValueWritable);
			}
		}
	}

	public static class dReducer
	extends Reducer<LongWritable, ArrayWritable, LongWritable, IntWritable> {

		public void reduce(LongWritable key, Iterable<ArrayWritable> values,
				Context context
				) throws IOException, InterruptedException {
			DoubleWritable min = new DoubleWritable(new Double(999.9));
			IntWritable i = new IntWritable(Integer.parseInt("0"));
			for(ArrayWritable arr : values){
				DoubleWritable d = (DoubleWritable) arr.get()[1];
				if(d.compareTo(min) == -1){
					min = d;
					i = (IntWritable) arr.get()[0];
				}
			}
			context.write(key, i);
		}
	}

	public static class cMapper
	extends Mapper<LongWritable, IntWritable, LongWritable, LongWritable>{
		public void map(LongWritable key, IntWritable value, Context context
				) throws IOException, InterruptedException {
			Long longValue = (long) value.get();
			LongWritable longWritableValue = new LongWritable(longValue);
			context.write(longWritableValue, key);
		}
	}

	public static class cReducer
	extends Reducer<LongWritable, LongWritable, LongWritable, ArrayWritable> {
		public void reduce(LongWritable key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			ArrayWritable outValueWritable = new ArrayWritable (DoubleWritable.class);
			DoubleWritable[] sum = new DoubleWritable[n];
			int count = 0;
			for(IntWritable point : values){
				for (int i = 0; i< n; i++){
					sum[i] = new DoubleWritable(new Double(sum[i].get()) + data.get(point)[i]);
				}
				count++;
			}
			for (int i = 0; i< n; i++){
				sum[i] = new DoubleWritable(new Double(sum[i].get())/count);
			}
			outValueWritable.set(sum);
			context.write(key, outValueWritable);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Invalid arguments");
			System.out.println("Arguments: \n \t k - number of clusters \n \t n - number of dimensions \n \t input Directory \n \t Output Directory");
			System.exit(0);
		}

		// load arguments
		k = Integer.parseInt(args[0]);
		n = Integer.parseInt(args[1]);

		// load data
		data = new HashMap<Integer, Double[]>();

		fileReader = new FileReader("mnist_data.txt");
		bufferedReader = new BufferedReader(fileReader);
		String strLine = "";
		//Read File Line By Line
		int key = 1;
		while ((strLine = bufferedReader.readLine()) != null)   {
			// split the line on your splitter(s)
			String[] splitted = strLine.split(" ");
			Double[] value = new Double[n];
			for (int count = 0; count < n; count++){
				Double d = new Double(Double.parseDouble(splitted[count]));
				value[count] = d;
			}
			data.put(key, value);
			key++;
		}
		//Close the input file
		fileReader.close();
		bufferedReader.close();

		//Initialize Centroids again with a HashMap
		prev=null;
		centroids=new HashMap<Integer, Double[]>();
		for (int i = 0 ; i<k; i++){
			Double[] centroid = new Double[n];
			for (int j = 0; j<n;j++){
				centroid[j] = new Double(i+1);
			}
			centroids.put(i+1, centroid);
		}
		ToolRunner.run(new Configuration(), new Kmeans(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		int count = 0 ;
		boolean isConverged = false;
		while(!isConverged){
			Configuration conf = new Configuration();
			Job djob = Job.getInstance(conf, "distanceMR"+count);
			djob.setJarByClass(Kmeans.class);
			djob.setMapperClass(dMapper.class);
			djob.setCombinerClass(dReducer.class);
			djob.setReducerClass(dReducer.class);
			djob.setOutputKeyClass(IntWritable.class);
			djob.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(djob, new Path(args[2]));
			FileOutputFormat.setOutputPath(djob, new Path("dOutput"+count));
			djob.waitForCompletion(true);

			Job cjob = Job.getInstance(conf, "centroidMR"+count);
			cjob.setJarByClass(Kmeans.class);
			cjob.setMapperClass(cMapper.class);
			cjob.setCombinerClass(cReducer.class);
			cjob.setReducerClass(cReducer.class);
			cjob.setOutputKeyClass(IntWritable.class);
			cjob.setOutputValueClass(ArrayWritable.class);
			FileInputFormat.addInputPath(cjob, new Path("dOutput"+count));
			FileOutputFormat.setOutputPath(cjob, new Path("cOutput"+count));
			cjob.waitForCompletion(true);
			cjob.waitForCompletion(true);

			prev = centroids;
			centroids = new HashMap<Integer, Double[]>();
			
			File folder = new File("cOutput"+count);
			File[] listOfFiles = folder.listFiles();
			for (int i = 0; i < listOfFiles.length; i++) {
				File file = listOfFiles[i];
				if (file.isFile()) {
					this.outFileReader = new FileReader(file);
					this.outBufferedReader = new BufferedReader(this.outFileReader);
					String strLine = "";
					//Read File Line By Line
					while ((strLine = this.outBufferedReader.readLine()) != null)   {
						// split the line on your splitter(s)
						String[] splitted = strLine.split(" ");
						Double[] value = new Double[n];
						for (int j = 0; j < n; j++){
							Double d = new Double(Double.parseDouble(splitted[j+1]));
							value[j] = d;
						}
						centroids.put(Integer.parseInt(splitted[0]), value);
					}
					//Close the input file
					this.outFileReader.close();
					this.outBufferedReader.close();
				}
			}
			isConverged = true;
			if (count != 0){
				for (int i=0; i<k; i++){
					for (int j = 0; j<n; j++){
						if(centroids.get(i)[j] != prev.get(i)[j]){
							isConverged = false;
							break;
						}
					}
					if (!isConverged){
						break;
					}
				}
			} else{
				isConverged = false;
			}
			count++;
		}
		// move files from one directory to another
		java.nio.file.Path FROM = Paths.get("cOutput"+count);
	    java.nio.file.Path TO = Paths.get(args[3]);
		CopyOption[] options = new CopyOption[]{
			      StandardCopyOption.REPLACE_EXISTING,
			      StandardCopyOption.COPY_ATTRIBUTES
			    }; 
		Files.copy(FROM, TO, options);
		return 1;
	}
}
