/**
 * @author Fabio Luchetti
 * @package pad.luchetti.pagerank
 */

package pad.luchetti.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3CheckConvergenceReducer extends Reducer<LongWritable, DoubleWritable, DoubleWritable, NullWritable> {

    private DoubleWritable kOut = new DoubleWritable();

    private double sum = 0.0;

    @Override
    public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) 
    		throws IOException, InterruptedException {
        
    	/* PageRank check convergence (reducer)
		
			 IN: 1 <tab> abs(Rank-sourcePrevRank)
			
			OUT: eps

         */
        for (DoubleWritable val : values) {
          sum += val.get();
        }

        kOut.set(sum);
        context.write(kOut, NullWritable.get());
        
    }
    


}
