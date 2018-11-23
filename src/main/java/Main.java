import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    String input1, input2, output;
    if (args.length == 3) {
      input1 = args[0];
      input2 = args[1];
      output = args[2];
    } else {
      System.err.println("Incorrect number of arguments.  Expected: input output");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(Main.class);
    job.setJobName(this.getClass().getName());

    MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class, RatingMapper.class);
    MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class, MovieMapper.class);
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.setReducerClass(MovieReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.setProperty("hadoop.home.dir", "/home/pawel/Pulpit/hadoop-3.1.1");
    Main driver = new Main();
    int exitCode = ToolRunner.run(driver, args);
    System.exit(exitCode);
  }
}