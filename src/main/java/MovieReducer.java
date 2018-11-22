import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovieReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
      InterruptedException {
    String title = "";
    double sum = 0;
    int counter = 0;

    for (Text value : values) {

      String s = value.toString();
      String trimValue = s.substring(2);
      if (s.matches("M~.*")) {
        title = trimValue;
      } else {
        sum += Double.valueOf(trimValue);
        counter += 1;
      }
    }

    String output = title +
        ", " +
        String.valueOf(Math.round(sum / counter * 100.0) / 100.0) +
        ", " +
        String.valueOf(counter);

    context.write(key, new Text(output));
  }
}