package Exercise1.Wanchoo1;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Wanchoo1_Herrera2_exercise extends Configured implements Tool {

	public static class KmeansMapper extends
			Mapper<Object, Text, IntWritable, DoubleArrayWritable> {

		HashMap<Integer, ArrayList<Double>> centersMap = new HashMap<Integer, ArrayList<Double>>();

		@SuppressWarnings("deprecation")
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			BufferedReader bReader;
			if (DistributedCache.getCacheFiles(context.getConfiguration()) != null
					&& DistributedCache.getCacheFiles(context
							.getConfiguration()).length > 0) {

				FileReader reader = new FileReader(new File("centroidData"));
				bReader = new BufferedReader(reader);

				for (String line = bReader.readLine(); line != null; line = bReader
						.readLine()) {
					ArrayList<Double> center = new ArrayList<Double>();
					String[] dimensions = line.split("\\s+");

					// 0th element of the split array is cluster number
					int cluster = Integer.parseInt(dimensions[0]);

					// 1st to length-1 elements go into the center array of
					// doubles
					for (int i = 1; i < dimensions.length; i++) {
						center.add(Double.parseDouble(dimensions[i].trim()));
					}

					// Put the <cluster_num, center> into hashmap centersMap
					centersMap.put(cluster, center);
				}
			}

		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			String[] columns = line.split("\\s+");
			ArrayList<Double> observation = new ArrayList<Double>();

			for (String obs : columns) {
				observation.add(Double.parseDouble(obs));
			}

			Double minDist = Double.MAX_VALUE;
			int ctrInd = -1;
			for (int i = 1; i <= centersMap.size(); i++) {
				if (observation.size() == centersMap.get(i).size()) {
					double dist = euclidean(observation, centersMap.get(i));
					if (dist < minDist) {
						minDist = dist;
						ctrInd = i;
					}
				}
			}

			long countKey = 1;
			context.write(new IntWritable(ctrInd), new DoubleArrayWritable(
					countKey, observation));
		}
	}

	public static double euclidean(ArrayList<Double> a, ArrayList<Double> b) {
		double sum = 0;
		for (int i = 0; i < a.size(); i++) {
			sum = sum + Math.pow((a.get(i) - b.get(i)), 2.0);
		}
		double dist = Math.sqrt(sum);
		return dist;
	}

	public static class DoubleArrayWritable implements Writable {
		private long count;
		private ArrayList<Double> data;

		public DoubleArrayWritable() {
		}

		public DoubleArrayWritable(long count, ArrayList<Double> data) {
			this.count = count;
			this.data = data;
		}

		public void write(DataOutput out) throws IOException {
			int length = 0;
			if (data != null) {
				length = data.size();
			}

			out.writeInt(length);
			out.writeLong(count);

			for (int i = 0; i < length; i++) {
				out.writeDouble(data.get(i));
			}
		}

		public void readFields(DataInput in) throws IOException {
			int length = in.readInt();
			count = in.readLong();
			data = new ArrayList<Double>(length);

			for (int i = 0; i < length; i++) {
				data.add(in.readDouble());
			}
		}
	}

	public static class KmeansCombiner
			extends
			Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {
		public void reduce(IntWritable key,
				Iterable<DoubleArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			long count = 0;

			ArrayList<Double> observations = null;
			for (DoubleArrayWritable obs : values) {
				if (observations == null) {
					observations = new ArrayList<Double>();

					for (int i = 0; i < obs.data.size(); i++) {
						observations.add(obs.data.get(i));
					}
					count += 1;
				} else {
					for (int i = 0; i < obs.data.size(); i++) {
						double tempVal = obs.data.get(i) + observations.get(i);
						observations.set(i, tempVal);
					}
					count += 1;
				}
			}
			context.write(key, new DoubleArrayWritable(count, observations));
		}
	}

	public static class KmeansReducer extends
			Reducer<IntWritable, DoubleArrayWritable, IntWritable, Text> {
		public void reduce(IntWritable key,
				Iterable<DoubleArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			long count = 0;

			ArrayList<Double> observations = null;
			for (DoubleArrayWritable obs : values) {
				if (observations == null) {
					observations = new ArrayList<Double>();

					for (int i = 0; i < obs.data.size(); i++) {
						observations.add(obs.data.get(i));
					}
					count += obs.count;
				} else {
					for (int i = 0; i < obs.data.size(); i++) {
						double tempVal = obs.data.get(i) + observations.get(i);
						observations.set(i, tempVal);
					}
					count += obs.count;
				}
			}
			
			ArrayList<Double> obsMean = new ArrayList<Double>(
					observations.size());

			for (int i = 0; i < observations.size(); i++) {
				obsMean.add(observations.get(i) / count);
			}

			StringBuilder newCtr = new StringBuilder();
			for (double dimMean: obsMean){
				newCtr.append(dimMean);
				newCtr.append(" ");
			}
			String newCtrString = newCtr.toString();

			context.write(key, new Text(newCtrString));
		}
	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("Kmeans clustering");

		job.setJarByClass(Wanchoo1_Herrera2_exercise.class);

		job.setMapperClass(KmeansMapper.class);
		job.setCombinerClass(KmeansCombiner.class);
		job.setReducerClass(KmeansReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleArrayWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		DistributedCache.addCacheFile(new URI(
				"kmeans/centroids.txt#centroidData"), job.getConfiguration());
		DistributedCache.createSymlink(job.getConfiguration());

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new Wanchoo1_Herrera2_exercise(), args);
		System.exit(res);
	}
}
