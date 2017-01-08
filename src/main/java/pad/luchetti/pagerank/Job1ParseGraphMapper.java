/**
 * @author Fabio Luchetti
 * @package pad.luchetti.pagerank
 */

package pad.luchetti.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Job1ParseGraphMapper extends Mapper<Object, Text, Text, Text> {
    
	private Text kOut = new Text();
	private Text vOut = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        /* PageRank parse graph (mapper)
		
		Job#1 mapper will parse chunks of the input file getting key-value pairs such as
		 
			 IN: Page <tab> Outlink

		denoting the edges of the web graph (directed from page to outlink).
		Comment lines, which begin with '#', are skipped. 
		In output it will produce:
		
			 OUT: Page <tab> CommaSeparatedOutlinks
		
         */
        
    	
        if (value.charAt(0) != '#') {
        	
        	String[] valueSplit = value.toString().split("\\t");
        	kOut.set(valueSplit[0]);
        	vOut.set(valueSplit[1]);
            context.write(kOut, vOut);
            
        }
 
    }
    
}
