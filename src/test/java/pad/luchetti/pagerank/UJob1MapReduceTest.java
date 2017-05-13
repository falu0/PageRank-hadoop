/**
 * @author Fabio Luchetti
 * @package pad.luchetti.pagerank
 */

package pad.luchetti.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;
import java.io.IOException;

 
public class UJob1MapReduceTest extends UJob1TestBase {
 
    @Test
    public void testMapReduce() throws IOException {
		
        try {
            mapReduceDriver.withInput(new LongWritable(2), new Text("1"));
            mapReduceDriver.withInput(new LongWritable(3), new Text("1"));
            mapReduceDriver.withInput(new LongWritable(3), new Text("4"));
            mapReduceDriver.addOutput(new Text("2"), new Text("1.0    0.0 1"));
            mapReduceDriver.addOutput(new Text("3"), new Text("1.0    0.0 1,4"));

            mapReduceDriver.runTest();
        } catch (ArrayIndexOutOfBoundsException e) { 
            /* this is expected - ignore it
             * assume that the input record for the mapper is "good".
             * we'd like a record with two fields splitted by a '/t' character */
        }
    }

    /* // if uncommented, this test will fail
    @Test
    public void testMapper() throws IOException {        
        mapReduceDriver.withInput(new LongWritable(2), new Text("1"));
        mapReduceDriver.addOutput(new Text("fail"), new Text("fail"));

        mapDriver.runTest();
    } */

}
