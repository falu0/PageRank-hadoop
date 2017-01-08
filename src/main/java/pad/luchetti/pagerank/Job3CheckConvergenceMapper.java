/**
 * @author Fabio Luchetti
 * @package pad.luchetti.pagerank
 */

package pad.luchetti.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Job3CheckConvergenceMapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
    
    private final static LongWritable ONE = new LongWritable(1);
	private DoubleWritable vOut = new DoubleWritable();
	
	private double partialSum = 0.0;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* PageRank check convergence (mapper)
			
			 IN: Page <tab> Rank <tab> prevRank <tab> CommaSeparatedOutlinks.
			
			OUT: 1 <tab> abs(Rank-prevRank)

         */
        
    	String[] valueSplit = value.toString().split("\\t");

    	//abs(Rank-prevRank)       
    	partialSum+=Math.abs(Double.parseDouble(valueSplit[1])-Double.parseDouble(valueSplit[2]));
        
        
    }
    
    public void cleanup(Context context) throws IOException, InterruptedException {
    	vOut.set(partialSum);
    	context.write(ONE, vOut);
    } 
    
}

