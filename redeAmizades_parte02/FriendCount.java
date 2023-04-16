import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendCount {

  public static class FriendCountMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{

    private Text person = new Text();
    private final static IntWritable one = new IntWritable(1);

    // Quebra a linha e emite um valor para cada palavra encontrada.
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] line = value.toString().split(","); // Separa os campos pela vírgula
      if (line.length == 2) {
        String[] friends = line[1].split("\\s+"); // Separa os amigos pelo espaço
        for (String friend : friends) {
          context.write(new Text(friend), one); // Emite cada amigo com o valor 1
        }
      }
    }
  }

  public static class FriendCountReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get(); // Soma os valores
      }
      context.write(key, new IntWritable(sum)); // Emite o amigo com a quantidade de amigos em comum
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "friend count");
    job.setJarByClass(FriendCount.class);
    job.setMapperClass(FriendCountMapper.class);
    job.setCombinerClass(FriendCountReducer.class);
    job.setReducerClass(FriendCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
