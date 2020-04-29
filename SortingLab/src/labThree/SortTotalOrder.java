package labThree;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortTotalOrder {

	public static void main(String[] args) throws Exception {

		// Set the number of reducer (No more than 10)
		int reduceNumber = 10;

		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: Patent <in> <out>");
			System.exit(2);
		}

		conf.setInt("Count", 0);

		CustomSampler sampler = new CustomSampler(otherArgs[0], "temp/_partitions");
		sampler.sample(1000, reduceNumber);

		// Set the path of partition file

		Job job = Job.getInstance(conf, "Exp1");
		job.setJarByClass(SortTotalOrder.class);
		job.setNumReduceTasks(reduceNumber);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(mapOne.class);
		job.setReducerClass(reduceOne.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setPartitionerClass(MyPartitioner.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		// Output path
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		if (job.waitForCompletion(true)) {
			/*
			 * 
			 * Note this deletes the temp directory, please comment this out
			 * if you would like to see my partitions.
			 * 
			 */
			
			FileSystem fs = FileSystem.get(conf);
			// Create path object and check for its existence 
			Path ParPath = new Path("temp");
			if (fs.exists(ParPath)) {
				fs.delete(ParPath, true); // false indicates do not deletes recursively
			}

			System.exit(0);
		}
	}

	public static class MyPartitioner extends Partitioner<Text, Text> {
		ArrayList<String> list;

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			int numSplits = numReduceTasks-1;
			if(list == null) {
				list = new ArrayList<String>();
				try {
					BufferedReader reader = new BufferedReader(new FileReader("temp/_partitions"));
					for(int i = 0; i < numSplits; i++) {
						list.add(reader.readLine());
					}
					
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			for(int i = 0; i < numSplits; i++) {
				if(key.toString().compareTo(list.get(i)) <= 0) return i;
			}
			return 9;
		}
	}

	public static class mapOne extends Mapper<LongWritable, Text, Text, Text> {

	}

	public static class reduceOne extends Reducer<Text, Text, Text, Text> {

	}
}