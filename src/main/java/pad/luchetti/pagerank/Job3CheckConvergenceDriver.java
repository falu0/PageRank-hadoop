/**
 * @author Fabio Luchetti
 * @package pad.luchetti.pagerank
 */

package pad.luchetti.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.GenericOptionsParser;


public class Job3CheckConvergenceDriver extends Configured implements Tool {
	
	private final Path input, output;
	private final boolean verbose;
	
	/** Initializes a new instance of the Job3CheckConvergenceDriver class */
	public Job3CheckConvergenceDriver(Path input, Path output, boolean verbose )
	{
		this.input = input;
		this.output = output;
		this.verbose = verbose;
	}
	
	
	/**
	 * This will run the Job #3 (Check Convergence).
	 * It calculates the estimated distance in l1-norm between the last rank-vector 
	 * and the current one. It is supposed to decrease after each iteration.
	 * @param args
 	 * @return <c>-1</c> if the Job failed its execution; <c>0</c> if everything is ok. 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException  
	 */
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		// GenericOptionsParser invocation in order to suppress the hadoop warning.
		//new GenericOptionsParser(conf, args);
		
		Job job = Job.getInstance(conf, "Job #3");
		job.setJarByClass(PageRank.class);

		// input / mapper
		FileInputFormat.setInputPaths(job, this.input);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setMapperClass(Job3CheckConvergenceMapper.class);
		
		// output / reducer
		FileOutputFormat.setOutputPath(job, this.output);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(NullWritable.class);
		//job.setNumReduceTasks(1);
		job.setReducerClass(Job3CheckConvergenceReducer.class);

		return (job.waitForCompletion(this.verbose)) ? 0 : -1;

	}

	
	/**
	 * Main method.
	 * @param args	array of external arguments,
	 * @throws IOException 
	 * @throws ClassNotFoundException
	 * @throws InterruptedException 
	 */
	public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException  {	
		
		if ( args.length != 3 ) {
			System.out.println( "Usage: Job3CheckConvergenceDriver <input> <output> <verbose>" );
			System.exit(1);
		}
		
		Job3CheckConvergenceDriver j = new Job3CheckConvergenceDriver( new Path(args[0]), new Path(args[1]), new Boolean(args[2]) );
		if ( j.run(null) != 0 )	{
			System.exit(1);
		}
		
		System.exit(0);
	}


}

