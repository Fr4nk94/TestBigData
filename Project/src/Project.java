

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Project {
	//auxiliary class which contain just the solver and one value of time associated to it
		public static class SolverInformation implements WritableComparable<SolverInformation> {

			private String solver;
			private String real;

			public SolverInformation() {
			}

			public SolverInformation(String solver, String time) {
				super();
				this.solver = solver;
				this.real = time;
			}

			@Override
			public void readFields(DataInput dataInput) throws IOException {
				solver = WritableUtils.readString(dataInput);
				real = WritableUtils.readString(dataInput);
			}

			@Override
			public void write(DataOutput out) throws IOException {
				WritableUtils.writeString(out, solver);
				WritableUtils.writeString(out, real);
			}

			@Override
			public int compareTo(SolverInformation o) {
				int cmp = solver.compareTo(o.solver);
				double a = Double.parseDouble(real);
				double b = Double.parseDouble(o.real);
				if (cmp == 0) {
					if (a > b)
						return 1;
					else if (a < b)
						return -1;
					else
						return 0;
				}
				return cmp;
			}
		}
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Job1");

		job1.setMapperClass(MapperFirstStep.class);
		job1.setReducerClass(RedurceFirstStep.class);
		job1.setGroupingComparatorClass(AggregatorSolverInformationsComparator.class);

		job1.setOutputKeyClass(SolverInformation.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		boolean success = job1.waitForCompletion(true);
		if (success) {
			Job job2 = Job.getInstance(conf, "Job2");

			job2.setMapperClass(AggregateMapper.class);
			job2.setReducerClass(AggregateReducer.class);

			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[2]));

			System.exit(job2.waitForCompletion(true) ? 0 : 1);

		} else {
			System.exit(1);
		}
	}
	
	//This mapper creates tuple like this:
	// (A,1.2),1.2
	//where the output will be an object done by two string
	//SolverName, Time
	static class MapperFirstStep extends Mapper<LongWritable, Text, SolverInformation, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, SolverInformation, Text>.Context context)
				throws IOException, InterruptedException {
			String[] row = value.toString().split("\t");
			
			
			String solver=row[0];
			String real=row[11];
			
			if (row[14].contains("solved")) {
				context.write(new SolverInformation(solver, real), new Text(real));
			}
		}
	}

	//The reducer create for each Solver a sort list for its value(real time)
	static class RedurceFirstStep extends Reducer<SolverInformation, Text, Text, Text> {
		@Override
		protected void reduce(SolverInformation key, Iterable<Text> values,
				Reducer<SolverInformation, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			StringBuilder str = new StringBuilder();
			
			
			
			for (Text value : values) {
				str.append(value.toString());
				str.append("\t");
			}
			
			
			context.write(new Text(key.solver), new Text(str.toString()));
		}
	}

	//The mapper takes in input the output of the other reducer and prepare data like:
	// 1 A	1.92	3.4		4.5	
	//where key 1 represent the index of the table we are building
	
	static class AggregateMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			for (int i = 1; i < split.length; i++) {
				context.write(new IntWritable(i), new Text(split[0] + "\t" + split[i]));
			}

		}
	}

	
	static class AggregateReducer extends Reducer<IntWritable, Text, Text, Text> {
		static boolean firstTime = true;

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			StringBuilder str = new StringBuilder();
			Map<String, String> solverReal = new HashMap<>();
			
			List<String> realsValues = new ArrayList<>();
			
			//in the map will insert the index and associated value
			for (Text value : values) {
				
				String[] type = value.toString().split("\t");
				if (!solverReal.containsKey(type[0])) {
					solverReal.put(type[0], type[1]);
					realsValues.add(type[0]);
				}
			}
			
			//sort the indices, so now I know how to represent the value
			realsValues.sort(new Comparator<String>() {
				public int compare(String arg0, String arg1) {
					return arg0.compareTo(arg1);
				};
			});
			
			//I need to know if it the first time in order to add the names of the solvers
			if (firstTime) {
				firstTime = false;
			//	str.append("\n\t");
				for (String s : realsValues) {
					str.append(s + "\t");

				}
				context.write(new Text(""), new Text(str.toString().substring(0, str.toString().length() - 2)));
				str = new StringBuilder("");
			}
			

			for (String s : realsValues) {
				str.append(solverReal.get(s) + "\t");
			}
			context.write(new Text(""),
					new Text(str.toString().substring(0, str.toString().length() - 2)));
		}
	}

	

//used to aggregate
	public static class AggregatorSolverInformationsComparator extends WritableComparator {

		protected AggregatorSolverInformationsComparator() {
			super(SolverInformation.class, true);
		}

		@Override
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			SolverInformation ac = (SolverInformation) a;
			SolverInformation bc = (SolverInformation) b;
			if (ac.solver.equals(bc.solver))
				return 0;
			return 1;
		}

	}

}