import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
	private BufferedReader br;

	public static class DoubleArrayWritable implements Writable {
		private Double[] doubleData;
		public DoubleArrayWritable() {
		}
		public DoubleArrayWritable(Double[] data) {
			this.doubleData = data;
		}
		public Double[] getData() {
			return this.doubleData;
		}
		public void setData(Double[] data) {
			this.doubleData = data;
		}
		public void write(DataOutput out) throws IOException {
			int length = 0;
			if(this.doubleData != null) {
				length = this.doubleData.length;
			}

			out.writeInt(length);

			for(int i = 0; i < length; i++) {
				out.writeDouble(this.doubleData[i]);
			}
		}
		public void readFields(DataInput in) throws IOException {
			int length = in.readInt();

			this.doubleData = new Double[length];

			for(int i = 0; i < length; i++) {
				this.doubleData[i] = in.readDouble();
			}
		}
	}

	public static class dMapper
	extends Mapper<LongWritable, Text, LongWritable, Writable>{

		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			for (int i = 0; i< 10000; i++){
				Double sum = 0.0;
				for (int j = 0; j< n; j++){
					sum = sum + Math.pow(data.get(i)[j] - centroids.get(key)[j], 2);
				}
				Double[] outValue = new Double[2];
				outValue[0] = new Double(key.get());
				outValue[1] = new Double(Math.sqrt(sum));
				DoubleArrayWritable outValueWritable = new DoubleArrayWritable(outValue);
				LongWritable outKey = new LongWritable((new Long(i)).longValue());
				context.write(outKey, outValueWritable);
			}
		}
	}

	public static class dReducer
	extends Reducer<LongWritable, ArrayWritable, LongWritable, IntWritable> {

		public void reduce(LongWritable key, Iterable<DoubleArrayWritable> values,
				Context context
				) throws IOException, InterruptedException {
			DoubleWritable min = new DoubleWritable(new Double(999.9));
			IntWritable i = new IntWritable(Integer.parseInt("0"));
			for(DoubleArrayWritable arr : values){
				DoubleWritable d = new DoubleWritable(arr.getData()[1]);
				if(d.compareTo(min) == -1){
					min = d;
					i = new IntWritable((arr.getData()[0]).intValue());
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
		public void reduce(LongWritable key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
			ArrayWritable outValueWritable = new ArrayWritable (DoubleWritable.class);
			DoubleWritable[] sum = new DoubleWritable[n];
			int count = 0;
			for(LongWritable point : values){
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
		File centroidsFile = new File("Centroids");

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
			djob.setMapOutputKeyClass(LongWritable.class);
			djob.setMapOutputValueClass(Writable.class);
			djob.setOutputKeyClass(LongWritable.class);
			djob.setOutputValueClass(ArrayWritable.class);
			if (count == 0){
				FileInputFormat.addInputPath(djob, new Path("harika/kmeans/centroids"));
			} else{
				FileInputFormat.addInputPath(djob, new Path("cOutput"+(count-1)));
			}
			FileOutputFormat.setOutputPath(djob, new Path("dOutput"+count));
			djob.waitForCompletion(true);

			Job cjob = Job.getInstance(conf, "centroidMR"+count);
			cjob.setJarByClass(Kmeans.class);
			cjob.setMapperClass(cMapper.class);
			cjob.setCombinerClass(cReducer.class);
			cjob.setReducerClass(cReducer.class);
			cjob.setMapOutputKeyClass(LongWritable.class);
			cjob.setMapOutputValueClass(LongWritable.class);
			cjob.setOutputKeyClass(IntWritable.class);
			cjob.setOutputValueClass(ArrayWritable.class);
			FileInputFormat.addInputPath(cjob, new Path("dOutput"+count));
			FileOutputFormat.setOutputPath(cjob, new Path("cOutput"+count));
			cjob.waitForCompletion(true);
			cjob.waitForCompletion(true);

			prev = centroids;
			centroids = new HashMap<Integer, Double[]>();

			Path ofile = new Path("cOutput"+count+"/part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
			this.br = new BufferedReader(new InputStreamReader(
					fs.open(ofile)));

			String strLine;
			while ((strLine = this.br.readLine()) != null)   {
				String[] splitted = strLine.split(" ");
				Double[] value = new Double[n];
				for (int j = 0; j < n; j++){
					Double d = new Double(Double.parseDouble(splitted[j+1]));
					value[j] = d;
				}
				centroids.put(Integer.parseInt(splitted[0]), value);
			}
			this.br.close();

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
		/*java.nio.file.Path FROM = Paths.get("cOutput"+count);
		java.nio.file.Path TO = Paths.get(args[3]);
		CopyOption[] options = new CopyOption[]{
				StandardCopyOption.REPLACE_EXISTING,
				StandardCopyOption.COPY_ATTRIBUTES
		}; 
		Files.copy(FROM, TO, options);*/
		return 1;
	}
}
