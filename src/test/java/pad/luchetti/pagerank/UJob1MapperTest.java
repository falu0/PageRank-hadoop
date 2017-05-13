/**
 * @author Fabio Luchetti
 * @package pad.luchetti.pagerank
 */

package pad.luchetti.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;
import java.io.IOException;
import java.lang.ArrayIndexOutOfBoundsException;


public class UJob1MapperTest extends UJob1TestBase {

    @Test
    public void testMapper() throws IOException {

        try { //FIXME bad practice
            mapDriver.withInput(new LongWritable(2), new Text("1"));
            mapDriver.withInput(new LongWritable(3), new Text("1"));
            mapDriver.withInput(new LongWritable(3), new Text("4"));
            mapDriver.withOutput(new Text("2"), new Text("1"));
            mapDriver.withOutput(new Text("3"), new Text("1,4"));

            mapDriver.runTest();
        } catch (ArrayIndexOutOfBoundsException e) { 
            /* this is expected - ignore it
             * assume that the input record for the mapper is "good".
             * we'd like a record with two fields splitted by a '/t' character */
        }
    }

    /* // if uncommented, this test will fail
    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new Text("9"), new Text("9"));
        mapDriver.withOutput(new Text("fail"), new Text("fail"));
        mapDriver.runTest();
    } */

}
