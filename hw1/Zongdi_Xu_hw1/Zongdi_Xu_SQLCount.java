

import java.io.File;

/**
 * @author crxon
 *
 */

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.hadoop.hdfs.tools.GetConf;

public class SQLCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    
    	String[] strList = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    	if (!strList[2].equals("facility_name")) {    		
    		word.set(String.format("%s,%s", strList[10], strList[2]));
//    		word.set(strList[10]);
    		context.write(word, one);
    	}
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  
  private static void deleteFileRecursively(File file) throws IOException {
		for (File childFile : file.listFiles()) {
			if (childFile.isDirectory()) {
				deleteFileRecursively(childFile);
			} else {
				if (!childFile.delete()) {
					throw new IOException();
				}
			}
		}

		if (!file.delete()) {
			throw new IOException();
		}
	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: SQLCount <in> <out>");
      System.exit(2);
    }
    
    try {
    	File outputFile = new File(otherArgs[otherArgs.length - 1]);
    	
    	if (outputFile.exists()) {
    		deleteFileRecursively(outputFile);
    		System.out.println(String.format("Directory '%s' already exists, deleted.", otherArgs[otherArgs.length - 1]));
    	}  	
    }
    catch (Exception e) {
    	System.err.println(e.getMessage());
    }
    

	//get configuration object
//	Configuration conf = getConf();
	 
	//set output delimiter
	conf.set("mapred.textoutputformat.separator", ":"); 
    
    Job job = Job.getInstance(conf, "SQLCount");
    job.setJarByClass(SQLCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    
    boolean completionStatus = job.waitForCompletion(true);
    System.out.println(completionStatus ? "Done." : "Die.");
    System.exit(completionStatus ? 0 : 1);
  }
}