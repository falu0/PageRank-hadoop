/**
 * @author Fabio Luchetti
 * @package pad.luchetti.pagerank
 */

package pad.luchetti.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapred.SkipBadRecords;


public class Job1ParseGraphDriver extends Configured implements Tool {
	
	private final Path input, output;
	private final boolean verbose;
	
	/** Initializes a new instance of the Job1ParseGraphDriver class */
	public Job1ParseGraphDriver(Path input, Path output, boolean verbose )
	{
		this.input = input;
		this.output = output;
		this.verbose = verbose;
	}
	
	
	/**
	 * This will run the Job #1 (Graph Parsing).
	 * It will parse the graph given as input and initialize the page ranks.
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
		
		Job job = Job.getInstance(conf, "Job #1");
		job.setJarByClass(PageRank.class);

		// input / mapper
		FileInputFormat.addInputPath(job, this.input);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(Job1ParseGraphMapper.class);

		// output / reducer
		FileOutputFormat.setOutputPath(job, this.output);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(Job1ParseGraphReducer.class);

		//skip max 100 bad input records for the mapper
		SkipBadRecords.setMapperMaxSkipRecords(conf, 100);

		return (job.waitForCompletion(this.verbose)) ? 0 : -1;

	}

	
	/**
	 * Main method.
	 * @param args	array of external arguments,
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public static void main( String[] args ) throws ClassNotFoundException, IOException, InterruptedException {	
		
		if ( args.length != 3 ) {
			System.out.println( "Usage: Job1ParseGraphDriver <input> <output> <verbose>" );
			System.exit(1);
		}
				
		Job1ParseGraphDriver j = new Job1ParseGraphDriver( new Path(args[0]), new Path(args[1]), new Boolean(args[2]) );
		if ( j.run(null) != 0 ) {
			System.exit(1);
		}
		
		System.exit(0);
	}


}
