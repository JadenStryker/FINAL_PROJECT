import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MmmMapper 
    extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable val = new IntWritable();
    private Text word = new Text();

    
    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
        
       
        String line = value.toString();
        String[] tokens = line.split(",", 0);
        word.set("90th percentile");
        val.set(Integer.parseInt(tokens[5]));
        context.write(word, val);
    }
        
        
        
        
    }

    

