/**
 * @author Fabio Luchetti
 * @package pad.luchetti.pagerank
 */

package pad.luchetti.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2CalculateRankReducer extends Reducer<Text, Text, Text, Text> {
    
	private Text vOut = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
                                                                                InterruptedException {
        
        /* PageRank calculate rank (reducer)
                		
			 IN: OutLink_j <tab> !sourceRank <tab> sourcePrevRank <tab> sourceOutlinkCount
			 IN: Page <tab> oldRank <tab> CommaSeparatedOutlinks
		
		The output store both the newly calculated rank and the previous one, in order to permit 
		(eventually, in another job) to test if the ranks values have converged.
		
			OUT: Page <tab> newRank <tab> oldRank <tab> CommaSeparatedOutlinks.
         
        */
        
    	double sourceRank = 0.0;
        int sourceOutlinkCount;
        double sumRanksShare = 0.0;

    	double oldRank = 0.0;
        String outlinks = null;

        String[] valueSplit;
        
        // For each page:
        // - check "control characters"
        // - calculate its pageRank share as: sourceRank/sourceOutlinkCount
        // - add the share to sumRanksShare
        for (Text value : values) {
 
            valueSplit = value.toString().split("\\t");
            
            if (valueSplit[0].startsWith(PageRank.SYM_RANK_SHARE)) {
                
//                System.out.println("RED gets if: " + key + " " + valueSplit[0] + " " + valueSplit[1]);
                sourceRank = Double.parseDouble(valueSplit[0].substring(PageRank.SYM_RANK_SHARE.length()));
                sourceOutlinkCount = Integer.parseInt(valueSplit[1]);
                
                // add the contributions from all the pages having an outlink pointing 
                // to the current node. The sum will be "damped down" later, before submitting the result.
                sumRanksShare += (sourceRank / sourceOutlinkCount);
                
            } else { //should enter once per key
                // if this value contains node links, append them to the 'outlinks' string
                // for future use. This is needed to reconstruct the correct input format
            	// for the next iterations of the Job.

//                System.out.println("RED gets else: " + key + " " + valueSplit[0] + " <outlinks>");
                oldRank = Double.parseDouble(valueSplit[0]);
            	if (valueSplit.length>1) 
            		outlinks = valueSplit[1];

            }

        }
        
        double newRank = (PageRank.DAMPING * sumRanksShare) + (1.0-PageRank.DAMPING);
        if (outlinks == null) {
        	vOut.set(newRank + "\t" + oldRank);
        	context.write(key, vOut);
        } else {
        	vOut.set(newRank + "\t" + oldRank + "\t" + outlinks);
        	context.write(key, vOut);
        }
//      System.out.println("REDUCE emit -> " + key + " " + newRank + "\t" + oldRank + "\t" + outlinks);

        
    }

}
