import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class A1Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        String []line = {"_", "_", "_"}; // Initialize and array of length 3
        String val = ""; // Empty string that will contain the key output of the reducer
        String k = ""; // Empty string that will contain the key/Mode of this column/category

        int max = 0; // Record the max count value
        String count = ""; // Record the count value to output
        for (Text value : values) { // Iterate through values
            line = value.toString().split("-"); // Parse line
            if (line.length == 3) { // If line is of correct length
                val = line[2]; // Record count of this line
                if (Integer.parseInt(val) > max) // If this count is greater than any other count
                {
                    max = Integer.parseInt(val); // Update max count value
                    count = val; // Record count as current output value (unless overturned by new value)
                }
            }
            k = line[1]; // Moved this down - outside of the nested loop
        }

        // Output formated column name, mode name, and count of that mode for each column
        context.write(new Text(key + "\t" + k), new Text(count));
    }
}
