

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;


public class Project {

	
	
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopN <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Project");
        job.setJarByClass(Project.class);
        job.setMapperClass(MapperColumn.class);
       // job.setCombinerClass(TopNCombiner.class);
       job.setReducerClass(ReduceColumn.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    
    //THIS MAPPER CREATE ELEMENT LIKE:
    // SOLVER   REAL_TIME
    //A			3.22
    //A			1.45
    //A			6.1
    //B			1.2
    static class MapperColumn extends  Mapper<LongWritable,Text,Text,Text> {
    	
    	private final int POSITION_OF_SOLVER_FIELD=0;
    	private final int POSITION_OF_REAL_FIELD=11;
    	
    	private int numberOfLine=0;
    	@Override
    	protected void map(LongWritable key, Text value,
    			Mapper<LongWritable, Text, Text, Text>.Context context)
    					throws IOException, InterruptedException {
    
    		if(numberOfLine!=0)//it means we are considering the header
    		{
    			String line=value.toString();
    					String [] split=line.split("\t");
    					
    					String solver=new String(split[POSITION_OF_SOLVER_FIELD]);
    					String realTime=new String(split[POSITION_OF_REAL_FIELD]);
    					String result=new String(split[14]);
    					
    					if(result.equals("solved"))
    					{
    						context.write(new Text(solver.toString()),new Text(realTime));
    					}
    		}
    		
    		
    			numberOfLine++;
    	
    	
    }
    }
    
    static class ReduceColumn extends Reducer<Text,Text,Text,Text> {
    	@Override
    	protected void reduce(Text solver, Iterable<Text> values,
    			Reducer<Text, Text, Text, Text>.Context context)
    					throws IOException, InterruptedException {
    		
    		StringBuilder val=new StringBuilder();
    		
    		
    		List<String>tempList=new ArrayList<>();
    		
    		for(Text text:values)
    		{
    			tempList.add(text.toString());
    		}
    		
    		Collections.sort(tempList);
    		Collections.reverse(tempList);
    		
    		for(String tmpString:tempList)
    		{
    			if(Double.parseDouble(tmpString)>1200)
    			{
    				val.append(1200+".0");
    			}
    			else{
    				val.append(tmpString);
    			}
    			
    			val.append("\t");
    		}
    		
    		context.write(solver, new Text(val.toString()));
    			
    		}
    	}
    	
    	
    	
    

    
}