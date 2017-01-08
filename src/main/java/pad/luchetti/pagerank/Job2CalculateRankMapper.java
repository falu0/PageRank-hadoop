/**
 * @author Fabio Luchetti
 * @package pad.luchetti.pagerank
 */

package pad.luchetti.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Job2CalculateRankMapper extends Mapper<LongWritable, Text, Text, Text> {
    
	
	private Text kOut = new Text();
	private Text vOut = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* PageRank calculate rank (mapper)
        
		The input format is:
			
			 IN: Page <tab> Rank <tab> prevRank <tab> CommaSeparatedOutlinks.
			
		Output format can be of two type:
		
			OUT: OutLink_j <tab> !sourceRank <tab> sourcePrevRank <tab> sourceOutlinkCount (page is not necessary)
			OUT: Page <tab> Rank <tab> CommaSeparatedOutlinks
			
         */
        
    	//could use directly array elements instead of declaring new vars, but lets choose readability
    	String[] valueSplit = value.toString().split("\\t");
        String page = valueSplit[0]; 
        String pageRank = valueSplit[1];
        //String prevPageRank = valueSplit[2];  
        String outlinks = ( valueSplit.length>3 ? valueSplit[3] : "" );
        
//        System.out.println("MAP gets: " + valueSplit[0] + " " + valueSplit[1] + " " + valueSplit[2] + " " + outlinks);

        if (!outlinks.isEmpty()) {
        	
            String[] outlinksSplit = outlinks.split(",");
            for (String outlink : outlinksSplit) { 
                kOut.set(outlink);
                vOut.set(PageRank.SYM_RANK_SHARE + pageRank + "\t" + outlinksSplit.length);
                context.write(kOut, vOut);
//              System.out.println("MAP emit -> " + outlink + " " + pageRanksWithTotalLinks);
            }

        }
        
        
        // put the original links so the reducer is able to produce the correct output
        kOut.set(page);
        vOut.set(pageRank + "\t" + outlinks);
        context.write(kOut, vOut);
//      System.out.println("MAP emit -> " + page + " " + pageRank + "\t" + outlinks);
    }
    
}

