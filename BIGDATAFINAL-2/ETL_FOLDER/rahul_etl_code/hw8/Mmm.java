import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Mmm {

    public static void main(String[] args) throws Exception { if (args.length != 2) {
        System.err.println("Usage: mean median mode <input path> <output path>");
        System.exit(-1);
      }
        Job job = new Job(); 
        job.setJarByClass(Mmm.class); 
        job.setJobName("mmm");
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1);
        job.setMapperClass(MmmMapper.class);
        job.setReducerClass(MmmReducer.class);
	job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1); }
}


