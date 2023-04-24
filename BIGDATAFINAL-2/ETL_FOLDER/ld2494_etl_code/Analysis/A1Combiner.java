import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class A1Combiner extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Extract the columns of interest
        String line[] = key.toString().split("-");
        int count = 0; // Counts the number of each ocurrence

        // If this is not the headers line
        // REMOVED else statement
        if (!line[0].equals("POPULATION:")) {
            for (Text value : values) { 
                count++; // Count Number of ocurrences
            }

            // Send the updated key (Column name) with the new value (Old key + count of this category) to the Reducer
            context.write(new Text(line[0]), new Text(key + "-" + String.valueOf(count)));
        }
    }
}
