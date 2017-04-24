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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.GenericOptionsParser;


public class Job4SortRankDriver extends Configured implements Tool {
	
	private final Path input, output;
	private final boolean verbose;
	
	/** Initializes a new instance of the Job4SortRankDriver class */
	public Job4SortRankDriver(Path input, Path output, boolean verbose )
	{
		this.input = input;
		this.output = output;
		this.verbose = verbose;
	}
	
	
	/**
	 * This will run the Job #4 (Rank Ordering).
	 * It will sort documents according to their page rank value.
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
		
		Job job = Job.getInstance(conf, "Job #4");
		job.setJarByClass(PageRank.class);

		// input / mapper
		FileInputFormat.setInputPaths(job, this.input);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(Job4SortRankMapper.class);

		// output
		FileOutputFormat.setOutputPath(job, this.output);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		return (job.waitForCompletion(this.verbose)) ? 0 : -1;

	}

	
	/**
	 * Main method.
	 * @param args	array of external arguments,
	 * @throws IOException 
	 * @throws ClassNotFoundException
	 * @throws InterruptedException 
	 */
	public static void main( String[] args ) throws Exception {	
		
		if ( args.length != 3 ) {
			System.out.println( "Usage: Job4SortRankDriver <input> <output> <verbose>" );
			System.exit(1);
		}
		
		Job4SortRankDriver j = new Job4SortRankDriver( new Path(args[0]), new Path(args[1]), new Boolean(args[2]) );
		if ( j.run(null) != 0 )	{
			System.exit(1);
		}
		
		System.exit(0);
	}


}

