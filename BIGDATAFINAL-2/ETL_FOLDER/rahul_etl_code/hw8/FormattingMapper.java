import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FormattingMapper 
    extends Mapper<LongWritable, Text, Text, NullWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    
    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
        
        if (value.toString().startsWith("State")) {
            return;
        }
        String line = value.toString();
        String[] tokens = line.split(",", 0);

        StringBuilder sb = new StringBuilder();
        String token = "";
        for(int i = 0; i < tokens.length; i++){
            if( i == 0 || i == 1) {
                token = tokens[i].toLowerCase();
            }
            else {
                token = tokens[i];
            }
            
            sb.append(token);
           
            sb.append(',');
            if (i == 5) {
                if (Integer.parseInt(token) <= 35) {
                    sb.append("0");
                }
                else {
                    sb.append("1");
                }
            }
        }
        word.set(sb.toString());
        
        context.write(word, NullWritable.get());
    }
        
        
        
        
    }

    

