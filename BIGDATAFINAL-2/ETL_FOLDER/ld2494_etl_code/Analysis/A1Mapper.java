import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Finds DISTINCT values in Year column as well as their counts
public class A1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input string to parse
        String line[] = value.toString().split("\t"); 
        
        // Extract the columns of interest
        String year = line[0];
        String state = line[1];
        String county = line[2];
        String pop = line[3];
        String areo = line[4];
        String offense = line[5];

        // If the row/line satisfies the following conditions (valid row for analysis)
        if ((!year.equals("YEAR")) && (!state.equals("STATE")) && (!county.equals("COUNTY")) && (!pop.equals("POP")) && (!areo.equals("AREO")) && (!offense.equals("OFFENSE"))) {
            // The keys below are formatted in a way to allow further desired processing by the combiner and reducer
            context.write(new Text("YEAR:-" + year), new Text("")); // Send Year info to combiner
            context.write(new Text("STATE:-" + state), new Text("")); // Send State info to combiner
            context.write(new Text("COUNTY:-" + county), new Text("")); // Send County info to combiner
            context.write(new Text("AREO:-" + areo), new Text("")); // Send Race/Ethnicity/Gender record status to combiner
            context.write(new Text("OFFENSE:-" + offense), new Text("")); // Send Offense info to combiner
        }
    }
}