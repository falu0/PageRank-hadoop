/**
 * @author Fabio Luchetti
 * @package pad.luchetti.pagerank
 */

package pad.luchetti.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1ParseGraphReducer extends Reducer<Text, Text, Text, Text> {
    
	private Text vOut = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        /* PageRank parse graph (reducer)
        
		The reducer gets, for each page, its outgoing links
		
			IN: Page <tab> CommaSeparatedOutlinks

		It will store the key-page with its initial PageRanks and its outgoing links.
		This output format is used as input format for the next job. 
			
			OUT: Page <tab> Rank <tab> prevRank <tab> CommaSeparatedOutlinks.

         */


        StringBuilder ranksAndOutlinks = new StringBuilder ("1.0\t0.0\t");
        
        for (Text value : values) {
        	ranksAndOutlinks.append(value.toString()+",");
        }
        
        //just remove trailing ","
        vOut.set(ranksAndOutlinks.substring(0, ranksAndOutlinks.length()-1));
        context.write(key, vOut);
    }

}
