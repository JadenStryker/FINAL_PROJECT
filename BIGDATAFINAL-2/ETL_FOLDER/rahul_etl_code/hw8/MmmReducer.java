import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MmmReducer
    extends Reducer<Text, IntWritable, Text, FloatWritable> {
	
    public List<Integer> list = new ArrayList<Integer>();
 
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
	     
            for (IntWritable val : values) {
                sum += val.get();
		count++;
		list.add(val.get());
            }
            float mean = (float) sum / count;
	    float median = list.get(count / 2);
           context.write(new Text("mean"), new FloatWritable(mean));
	   context.write(new Text("median"), new FloatWritable(median));

    }
}
