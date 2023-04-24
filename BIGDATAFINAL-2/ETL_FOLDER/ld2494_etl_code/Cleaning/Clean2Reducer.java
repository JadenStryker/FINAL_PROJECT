import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Clean2Reducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String []line;
        int count = 0;
        int pop = 1;
        int off_0 = 0;
        int off_1 = 0;
        int off_2 = 0;
        int off_3 = 0;
        for (Text value : values) { 
            line = value.toString().split("\t"); // pop, off0, off1, off2, off3

            pop = Integer.parseInt(line[0]);
            off_0 += Integer.parseInt(line[1]); // More severe offense gets lower number (0)
            off_1 += Integer.parseInt(line[2]);
            off_2 += Integer.parseInt(line[3]);
            off_3 += Integer.parseInt(line[4]); // Less severe offense gets higher number (3)
        }

        int total = off_0 + off_1 + off_2 + off_3;
        String offenses = total + "\t" + off_0 + "\t" + off_1 + "\t" + off_2 + "\t" + off_3;
        float ratePer100K = 100000*(((float)total)/pop);

        // Write the result to the output file
        context.write(key, new Text(pop + "\t" + offenses + "\t" + ratePer100K));
    }
}
