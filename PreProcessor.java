import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PreProcessor extends Configured implements Tool {
	public static class Map extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			System.out.println(conf.get("provider_type"));
			String provider_type = conf.get("provider_type");

			String instance = value.toString();
			String[] icoords = instance.split("\t");
			if (icoords.length >= 26) {
				String ptype = icoords[13].trim();

				StringBuilder clustValues = new StringBuilder();
				String avgSubmitted = icoords[23];
				String stdSubmitted = icoords[24];
				String avgRecd = icoords[25];

				if (!avgSubmitted.isEmpty() && !stdSubmitted.isEmpty()
						&& !avgRecd.isEmpty()) {
					clustValues.append(avgSubmitted);
					clustValues.append("\t");
					clustValues.append(stdSubmitted);
					clustValues.append("\t");
					clustValues.append(avgRecd);
				}

				String strValues = clustValues.toString();

				if (provider_type.trim().equals(ptype)) {
					context.write(value, new Text(strValues));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(NullWritable.get(), new Text(val.toString()));
			}
		}

	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("provider_type", args[0]);
		Job job = Job.getInstance(conf);
		job.setJobName("Medicare PreProcessor");

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(PreProcessor.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PreProcessor(), args);
		System.exit(res);
	}
}
