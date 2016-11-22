import java.io.BufferedReader;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Kmeans extends Configured implements Tool  {
	static HashMap<Integer, Double[]> data;
	static HashMap<Integer, Double[]> prev;
	static HashMap<Integer, Double[]> centroids;
	static int k=10;
	static int n=784;
	private static FileReader fileReader;
	private static BufferedReader bufferedReader;
	private BufferedReader br;
	private Logger mainLogger = Logger.getLogger(Kmeans.class);

	// Own class to pass double array from map to reduce
	public static class DoubleArrayWritable extends ArrayWritable {
		public DoubleArrayWritable() {
			super(DoubleWritable.class);
		}
	}

	// distance mapper
	// Output: key: datapointindex, value: [indexofcentroid distancetothatcentroid]
	public static class dMapper
	extends Mapper<LongWritable, Text, LongWritable, DoubleArrayWritable>{

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			int cIndex = Integer.parseInt(value.toString().split("\\s+")[0]);
			//loop through all data points
			for (int i = 0; i< 10000; i++){
				Double sum = 0.0;
				for (int j = 0; j< n; j++){
					// calculate sum of squares
					sum = sum + Math.pow(data.get(i)[j] - centroids.get(cIndex)[j], 2);
				}
				DoubleArrayWritable outValueWritable = new DoubleArrayWritable ();
				DoubleWritable[] outValue = new DoubleWritable[2];
				outValue[0] = new DoubleWritable((new Double(cIndex)).doubleValue()); // index of the centroid
				outValue[1] = new DoubleWritable(Math.sqrt(sum));// distance to that centroid
				outValueWritable.set(outValue);
				LongWritable outKey = new LongWritable((new Long(i)).longValue());
				context.write(outKey, outValueWritable);
			}
		}
	}

	// distance calculator reducer
	// finds the nearest centroid and 
	// gives the output: key: datapointindex, value: centroidindex
	public static class dReducer
	extends Reducer<LongWritable, DoubleArrayWritable, LongWritable, IntWritable> {

		@Override
		public void reduce(LongWritable key, Iterable<DoubleArrayWritable> values,
				Context context
				) throws IOException, InterruptedException {
			DoubleWritable min = new DoubleWritable(new Double(999.9));
			IntWritable i = new IntWritable(Integer.parseInt("0"));
			for(DoubleArrayWritable arr : values){
				DoubleWritable d = (DoubleWritable) arr.get()[1];
				if(d.compareTo(min) == -1){
					min = d;
					i = new IntWritable(new Double(((DoubleWritable)(arr.get()[0])).get()).intValue());
				}
			}
			context.write(key, i);
		}
	}

	// Centroid calculator 
	// input: dreducer output
	// input: Key: datapointindex, value: centroidindex
	// output: Key: centroidindex, value: datapointindex
	public static class cMapper
	extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			Long longValue = new Long(Long.parseLong(value.toString().split("\\s+")[1]));
			LongWritable longWritableValue = new LongWritable(longValue);
			Long longValue2 = new Long(Long.parseLong(value.toString().split("\\s+")[0]));
			LongWritable longWritableValue2 = new LongWritable(longValue2);
			context.write(longWritableValue, longWritableValue2);
		}
	}

	// Centroid Calculator reducer
	// input: Key: centroidindex, values: [group of datapoints in that cluster]
	// output: Key: centroidindex, value: [new centroid point value]
	public static class cReducer
	extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
		private Logger logger = Logger.getLogger(cReducer.class);
		@Override
		public void reduce(LongWritable key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
			Double[] sum = new Double[n];
			this.logger.error("CREDUCER n: "+ n);
			for (int i = 0; i< n; i++){
				sum[i] = new Double(0);
				this.logger.error("CREDUCER SUM[i]: "+ sum[i].toString().trim());
			}
			int count = 1;
			for(LongWritable point : values){
				for (int i = 0; i< n; i++){
					String dindexstr = new Long(point.get()).toString();
					this.logger.error("CREDUCER dinedxstr: "+ dindexstr);
					int dindex = Integer.parseInt(dindexstr);
					this.logger.error("CREDUCER dindex: "+ dindex);
					Double d = data.get(dindex)[i];
					this.logger.error("CREDUCER d: "+ d);
					sum[i] = sum[i] + d;
					this.logger.error("CREDUCER d+SUM[i]: "+ sum[i].toString().trim());
				}
				count++;
			}
			this.logger.error("CREDUCER count: "+ count);
			StringBuffer outString = new StringBuffer();
			for (int i = 0; i< n; i++){
				sum[i] = sum[i]/count;
				outString.append(" 1 ");
				outString.append(sum[i].toString());
			}
			this.logger.error("CREDUCER OUTSTRING: "+ outString.toString().trim());
			context.write(key, new Text(outString.toString().trim()));
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
		n = 784;

		// load data
		data = new HashMap<Integer, Double[]>();

		// load the data from file
		fileReader = new FileReader("mnist_data.txt");
		bufferedReader = new BufferedReader(fileReader);
		String strLine = "";
		//Read File Line By Line
		int key = 1;
		while ((strLine = bufferedReader.readLine()) != null)   {
			// split the line on your splitter(s)
			String[] splitted = strLine.split("\\s+");
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
			// initialize centroids in the format of <1,1,1,1,....>, <2,2,2,2,....>,....
			centroids.put(i+1, centroid);
		}
		ToolRunner.run(new Configuration(), new Kmeans(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		int count = 0 ;
		boolean isConverged = false;
		while(!isConverged){

			// Distance calculator job
			Configuration conf = new Configuration();
			/*Job djob = Job.getInstance(conf, "distanceMR"+count);
			djob.setJarByClass(Kmeans.class);
			djob.setMapperClass(dMapper.class);
			djob.setReducerClass(dReducer.class);
			djob.setMapOutputKeyClass(LongWritable.class);
			djob.setMapOutputValueClass(DoubleArrayWritable.class);
			djob.setOutputKeyClass(LongWritable.class);
			djob.setOutputValueClass(IntWritable.class);
			// if this is first iteration, read from centroids file
			if (count == 0){
				FileInputFormat.addInputPath(djob, new Path("harika/kmeans/centroids"));
			} else{
				// else, read from previous output, basically its the same, as we are only considering indexes.
				FileInputFormat.addInputPath(djob, new Path("cOutput"+(count-1)));
			}
			FileOutputFormat.setOutputPath(djob, new Path("dOutput"+count));
			djob.waitForCompletion(true);*/

			// Second Mapreduce job - to calculate new centroids
			Job cjob = Job.getInstance(conf, "centroidMR"+count);
			cjob.setJarByClass(Kmeans.class);
			cjob.setMapperClass(cMapper.class);
			//cjob.setCombinerClass(cReducer.class);
			cjob.setReducerClass(cReducer.class);
			cjob.setMapOutputKeyClass(LongWritable.class);
			cjob.setMapOutputValueClass(LongWritable.class);
			cjob.setOutputKeyClass(LongWritable.class);
			cjob.setOutputValueClass(Text.class);
			// input is output from previous mapreduce distance calculator job
			FileInputFormat.addInputPath(cjob, new Path("dOutput"+count));
			FileOutputFormat.setOutputPath(cjob, new Path("cOutput"+count));
			cjob.waitForCompletion(true);

			// backup the centroids
			prev = centroids;
			centroids = new HashMap<Integer, Double[]>();

			// read the output from the second mapreduce job
			Path ofile = new Path("cOutput"+count+"/part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
			this.br = new BufferedReader(new InputStreamReader(
					fs.open(ofile)));

			String strLine;
			while ((strLine = this.br.readLine()) != null)   {
				String[] splitted = strLine.split("\\s+");
				Double[] value = new Double[n];
				for (int j = 0; j < n; j++){
					Double d = new Double(Double.parseDouble(splitted[j+1]));
					value[j] = d;
				}
				// load the values into centroids variable
				centroids.put(Integer.parseInt(splitted[0]), value);
			}
			this.br.close();

			//check if the clustering is converged?
			isConverged = true;
			if (count != 0){
				for (int i=0; i<k; i++){
					for (int j = 0; j<n; j++){
						// if all the centroid values are same, it is converged.
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
				// if not converged, repeat the jobs again for new centroids
			}
			count++;
		}
		// move files from one directory to another
		Path FROM = new Path("cOutput"+count);
		Path TO = new Path(args[3]);
		FileSystem fs = FileSystem.get(new Configuration());
		fs.rename(FROM, TO);
		return 1;
	}
}
