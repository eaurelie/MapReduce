import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Pivot {

public static class Map extends Mapper<Object, Text,IntWritable , Text> {
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          String[] tableaux = value.toString().split(",");
 for (int i=0; i<tableaux.length ; i++)
          {
   context.write(new  IntWritable(i) , new Text(tableaux[i]));
          }
       }
}
public static class Reduce extends Reducer<IntWritable,Text,IntWritable,Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

String row = "";
for (Text val : values)
    {
           row += val.toString() + ",";
     }
           String res = row.substring(0, row.length - 1);
            context.write(key, new Text(res));
   }
}

public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = Job.getInstance(conf, "pivot");
 job.setJarByClass(Pivot.class);
   job.setMapperClass(Map.class);
 job.setCombinerClass(Reduce.class);
 job.setReducerClass(Reduce.class);
     job.setOutputKeyClass(IntWritable.class);
  job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
