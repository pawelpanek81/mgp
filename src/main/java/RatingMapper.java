import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

  private static Pattern inputPattern = Pattern.compile("(\\d*),(\\d*),(\\d.\\d),(\\d*)");
  private static final String fileTag = "R~";


  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String inputLine = value.toString();

    Matcher inputMatch = inputPattern.matcher(inputLine);

    if (inputMatch.matches()) {
      String movieId = inputMatch.group(2);
      String rating = inputMatch.group(3);

      context.write(new Text(movieId), new Text(fileTag + rating));
    }
  }
}