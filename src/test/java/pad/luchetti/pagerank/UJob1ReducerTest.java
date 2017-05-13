/**
 * @author Fabio Luchetti
 * @package pad.luchetti.pagerank
 */

package pad.luchetti.pagerank;

import org.apache.hadoop.io.Text;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;
import java.io.IOException;

 
public class UJob1ReducerTest extends UJob1TestBase{
   
    @Test
    public void testReducer() throws IOException {
		
		List<Text> values2 = new ArrayList<Text>();
		values2.add(new Text("1"));		
        reduceDriver.withInput(new Text("2"), values2);
        reduceDriver.withOutput(new Text("2"), new Text("1.0	0.0	1"));

		List<Text> values3 = new ArrayList<Text>();
		values3.add(new Text("1"));
		values3.add(new Text("4"));
        reduceDriver.withInput(new Text("3"), values3);
        reduceDriver.withOutput(new Text("3"), new Text("1.0	0.0	1,4"));

        reduceDriver.runTest();
    }

    /* // if uncommented, this test will fail
    @Test
    public void testMapper() throws IOException {        
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("9"));     
        reduceDriver.withInput(new Text("9"), values);
        reduceDriver.withOutput(new Text("fail"), new Text("fail"));

        mapDriver.runTest();
    } */

}
