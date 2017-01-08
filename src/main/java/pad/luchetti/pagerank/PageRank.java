/**
 * @author Fabio Luchetti
 * @package pad.luchetti.pagerank
 */


package pad.luchetti.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PageRank extends Configured implements Tool {

	/* args keys */
	private static final String KEY_DAMPING = "--damping";
	private static final String KEY_DAMPING_ALIAS = "-d";

	private static final String KEY_COUNT = "--count";
	private static final String KEY_COUNT_ALIAS = "-c";

	private static final String KEY_INPUT = "--input";
	private static final String KEY_INPUT_ALIAS = "-i";

	private static final String KEY_OUTPUT = "--output";
	private static final String KEY_OUTPUT_ALIAS = "-o"; 

	private static final String KEY_HELP = "--help";
	private static final String KEY_HELP_ALIAS = "-h"; 

	private static final String KEY_ACCURACY = "--accuracy";
	private static final String KEY_ACCURACY_ALIAS = "-a"; 

	private static final String KEY_PERIODICITY = "--periodicity";
	private static final String KEY_PERIODICITY_ALIAS = "-p"; 


	/* utility attributes */
	public static String SYM_RANK_SHARE = "!";

	// configuration values
	public static double DAMPING = 0.85;
	public static int MAX_ITERATIONS = 3;
	public static String IN_PATH = "";
	public static String OUT_PATH = "";
	public static double ACCURACY = 1.0;
	private static int CONVERGENCE_TEST_PERIODICITY = 2; //if 0, no test



	/**
	 * This is the main class run against the Hadoop cluster.
	 * It will launch all the jobs needed for the PageRank algorithm.
	 */
    @Override
    public int run(String[] args) throws Exception {
    	
    	parseInput(args);

		/* By default an Hadoop job won't start if the output path already exists.
        To fasten the tests, add a clause for local mode execution: 
        if the specified output path is named "test" and it already exists,
        delete it before running the job. */
		FileSystem fs = FileSystem.get(new Configuration());
		if ("test".equals(PageRank.OUT_PATH) && fs.exists(new Path(PageRank.OUT_PATH)))
			fs.delete(new Path(PageRank.OUT_PATH), true);

		// print current configuration in the console
		System.out.println("****************************************");
		System.out.println("Damping factor: " + PageRank.DAMPING);
		System.out.println("Max iterations: " + PageRank.MAX_ITERATIONS);
		System.out.println("Accuracy: " + PageRank.ACCURACY);
		System.out.println("Chek for convergence every p iteration, p: " + PageRank.CONVERGENCE_TEST_PERIODICITY);
		System.out.println("Input directory: " + PageRank.IN_PATH);
		System.out.println("Output directory: " + PageRank.OUT_PATH);
		System.out.println("****************************************");

		Thread.sleep(1000);
		
		
		String inPath, lastOutPath = null;
		
        // to check the accuracy
		String epsOutPath;
		BufferedReader eps_br;
        String eps_line;
        double eps;

        
		boolean isCompleted = false;
		PageRank pagerank = new PageRank();

		System.out.println("\nRunning Job#1 (graph parsing) ...\n");
		isCompleted = pagerank.job1(IN_PATH, OUT_PATH + "/iter00");
		if (!isCompleted) {
			System.exit(1);
		}

		/* 
		 * Iterate the MapReduce job to update the PageRanks.
		 * Every p iterations compute the new estimate of error and check for convergence
		 */
		for (int i = 0; i < MAX_ITERATIONS; i++) {
			inPath = OUT_PATH + "/iter" + (String.format("%02d", i));
			lastOutPath = OUT_PATH + "/iter" + (String.format("%02d", i+1));
			System.out.println("\nRunning Job#2 (PageRank calculation), iteration no. " + (i+1) + " ...\n");
			System.out.println("inPath:" + inPath + " lastOutPath:" + lastOutPath);
			isCompleted = pagerank.job2(inPath, lastOutPath);
			if (!isCompleted) {
				System.exit(1);
			}
			if ( i>0 && ((i%PageRank.CONVERGENCE_TEST_PERIODICITY) == 0) ) { //checks for convergence
				System.out.println("\nRunning Job#3 (check convergence) ...\n");
				epsOutPath = OUT_PATH + "/eps" + (String.format("%02d", i+1));
				
				isCompleted = pagerank.job3(lastOutPath, epsOutPath);
				if (!isCompleted) {
					System.exit(1);
				}

				//double eps = Double.parseDouble(fs.open(new Path(epsOutPath +"/part-r-00000")).readLine()); *readLine()* is deprecated
                eps_br = new BufferedReader(new InputStreamReader(fs.open(new Path(epsOutPath +"/part-r-00000"))));
                eps_line = eps_br.readLine();
                if (eps_br.readLine() != null) {
                	System.err.println("eps file should be 1 line long (containing a double)");
                	System.exit(1);                	
                }
                eps = Double.parseDouble(eps_line);
				//System.out.println("eps: "+ eps);
    			System.out.println("Computed distance from previous iteration is " + eps);
				if (eps < PageRank.ACCURACY) {//done
					System.out.println("It converges! Stop iterating at i=" + i);
					i = MAX_ITERATIONS;
				} else if ((eps/PageRank.ACCURACY) < (10*PageRank.ACCURACY)) {//almost done
					//Fasten the checks. Could be done more precise/parametric; nevertheless, the idea is the same
					PageRank.CONVERGENCE_TEST_PERIODICITY = Math.round(1 + (int) PageRank.CONVERGENCE_TEST_PERIODICITY/2);
				}


			}
		}


		System.out.println("\nRunning Job#4 (rank ordering) ...\n");
		isCompleted = pagerank.job4(lastOutPath, OUT_PATH + "/result");
		if (!isCompleted) {
			System.exit(1);
		}

		System.out.println("DONE!");
		return 0;
    }

	public static void main(String[] args) throws Exception {
	        System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
	}
	
	/**
	 * Parse the args in input and assing the passed values to the proper variables 
	 * 
	 * @param args
	 */
	public static void parseInput(String[] args) {

		try {
			for (int i = 0; i < args.length; i += 2) {

				String key = args[i];
				String value = args[i + 1];

				// NOTE: do not use a switch to keep Java 1.6 compatibility!
				if (key.equals(KEY_DAMPING) || key.equals(KEY_DAMPING_ALIAS)) {
					// be sure to have a damping factor in the interval [0:1]
					PageRank.DAMPING = Math.max(Math.min(Double.parseDouble(value), 1.0), 0.0);
				} else if (key.equals(KEY_COUNT) || key.equals(KEY_COUNT_ALIAS)) {
					// be sure to have at least 1 iteration for the PageRank algorithm
					PageRank.MAX_ITERATIONS = Math.max(Integer.parseInt(value), 1);
				} else if (key.equals(KEY_INPUT) || key.equals(KEY_INPUT_ALIAS)) {
					PageRank.IN_PATH = value.trim();
					if (PageRank.IN_PATH.charAt(PageRank.IN_PATH.length() - 1) == '/')
						PageRank.IN_PATH = PageRank.IN_PATH.substring(0, PageRank.IN_PATH.length() - 1);
				} else if (key.equals(KEY_OUTPUT) || key.equals(KEY_OUTPUT_ALIAS)) {
					PageRank.OUT_PATH = value.trim();
					if (PageRank.OUT_PATH.charAt(PageRank.OUT_PATH.length() - 1) == '/')
						PageRank.OUT_PATH = PageRank.OUT_PATH.substring(0, PageRank.IN_PATH.length() - 1);
				} else if (key.equals(KEY_ACCURACY) || key.equals(KEY_ACCURACY_ALIAS)) {
					PageRank.ACCURACY = Double.parseDouble(value);
				} else if (key.equals(KEY_PERIODICITY) || key.equals(KEY_PERIODICITY_ALIAS)) {
					PageRank.CONVERGENCE_TEST_PERIODICITY = Integer.parseInt(value);
				} else if (key.equals(KEY_HELP) || key.equals(KEY_HELP_ALIAS)) {
					printHelp(null);
					System.exit(0);                        
				}
			}            
		} catch (ArrayIndexOutOfBoundsException e) {
			printHelp(e.getMessage());
			System.exit(1);
		} catch (NumberFormatException e) {
			printHelp(e.getMessage());
			System.exit(1);
		}

		// check for valid parameters to be set
		if (PageRank.IN_PATH.isEmpty() || PageRank.OUT_PATH.isEmpty()) {
			printHelp("missing required parameters");
			System.exit(1);
		}


	}

	/**
	 * This will run the Job #1 (Graph Parsing).
	 * Will parse the graph given as input and initialize the page rank.
	 * 
	 * @param in the directory of the input data
	 * @param out the directory of the output
	 */
	public boolean job1(String in, String out) throws IOException, 
	ClassNotFoundException, 
	InterruptedException {
		Job job = Job.getInstance(new Configuration(), "Job #1");
		job.setJarByClass(PageRank.class);

		// input / mapper
		FileInputFormat.addInputPath(job, new Path(in));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(Job1ParseGraphMapper.class);

		// output / reducer
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(Job1ParseGraphReducer.class);

		return job.waitForCompletion(false);

	}

	/**
	 * This will run the Job #2 (Rank Calculation).
	 * It calculates the new ranking and generates the same output format as the input, 
	 * so this job can run multiple times (more iterations will increase accuracy).
	 * 
	 * @param in the directory of the input data
	 * @param out the directory of the output
	 */
	public boolean job2(String in, String out) throws IOException, 
	ClassNotFoundException, 
	InterruptedException {

		Job job = Job.getInstance(new Configuration(), "Job #2");
		job.setJarByClass(PageRank.class);

		// input / mapper
		FileInputFormat.setInputPaths(job, new Path(in));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(Job2CalculateRankMapper.class);

		// output / reducer
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(Job2CalculateRankReducer.class);

		return job.waitForCompletion(false);

	}

	/**
	 * This will run the Job #3 (Check Convergence).
	 * It calculates the estimated distance in l1-norm between the last rank-vector 
	 * and the current one. It is supposed to decrease after each iteration.
	 * 
	 * @param in the directory of the input data
	 * @param out the directory of the output
	 */
	public boolean job3(String in, String out) throws IOException,
	ClassNotFoundException,
	InterruptedException {


		Job job = Job.getInstance(new Configuration(), "Job #3");
		job.setJarByClass(PageRank.class);

		// input / mapper
		FileInputFormat.setInputPaths(job, new Path(in));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setMapperClass(Job3CheckConvergenceMapper.class);
		
		// output / reducer
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(NullWritable.class);
		//job.setNumReduceTasks(1);
		job.setReducerClass(Job3CheckConvergenceReducer.class);

		return job.waitForCompletion(false);

	}

	/**
	 * This will run the Job #4 (Rank Ordering).
	 * It will sort documents according to their page rank value.
	 * 
	 * @param in the directory of the input data
	 * @param out the directory of the output
	 */
	public boolean job4(String in, String out) throws IOException, 
	ClassNotFoundException, 
	InterruptedException {

		Job job = Job.getInstance(new Configuration(), "Job #4");
		job.setJarByClass(PageRank.class);

		// input / mapper
		FileInputFormat.setInputPaths(job, new Path(in));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(Job4SortRankMapper.class);

		// output
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(false);

	}

	/**
	 * Print an help text in System.out
	 * 
	 * @param err an optional error message to display
	 */
	public static void printHelp(String err) {

		if (err != null) {
			// if error has been given, print it
			System.err.println("ERROR: " + err + ".\n");
		}

		System.out.println("Usage: pagerank.jar " + KEY_INPUT + " <input> " + KEY_OUTPUT + " <output>\n");
		System.out.println("Options:\n");
		System.out.println(" " + KEY_INPUT + "\t(" + KEY_INPUT_ALIAS + ") \t<input> \tThe directory of the input graph [REQUIRED]");
		System.out.println(" " + KEY_OUTPUT + "\t(" + KEY_OUTPUT_ALIAS + ") \t<output> \tThe directory of the output result [REQUIRED]");
		System.out.println(" " + KEY_DAMPING + "\t(" + KEY_DAMPING_ALIAS + ") \t<damping> \tThe damping factor [OPTIONAL]. Default is 0.85");
		System.out.println(" " + KEY_COUNT + "\t(" + KEY_COUNT_ALIAS + ") \t<max iterations> \tThe maximum amount of iterations [OPTIONAL]. Default is 2");
		System.out.println(" " + KEY_ACCURACY + "\t(" + KEY_ACCURACY_ALIAS + ") \t<accuracy> \tThe estimate of error in norm 1 for the rank vector [OPTIONAL]. Default is 0.001");
		System.out.println(" " + KEY_PERIODICITY + "\t(" + KEY_PERIODICITY_ALIAS + ") \t<periodicity> \tChecks for convergence every <p> rank-calculation iterations [OPTIONAL]. Default is 3");
		System.out.println(" " + KEY_HELP + "\t\t(" + KEY_HELP_ALIAS + ") \tDisplay this help text\n");


	}



}

