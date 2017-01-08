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

public class Job4SortRankMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    
	private DoubleWritable kOut = new DoubleWritable();
	private Text vOut = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* PageRank rank ordering (mapper only)
         
         This job lacks the reducer phase. It will simply store the proper rank for each page , 
         and we will exploit the Hadoop automatic key-sorting mechanism to give order to the result.
         
			 IN: Page <tab> Rank <tab> prevRank <tab> CommaSeparatedOutlinks.
			 
			OUT: Rank <tab> Page
         
         */
        
    	String[] valueSplit = value.toString().split("\\t"); //Rank, Page
    	
    	kOut.set(Double.parseDouble(valueSplit[1]));
    	vOut.set(valueSplit[0]);
        context.write(kOut, vOut);
        
    }
       
}
