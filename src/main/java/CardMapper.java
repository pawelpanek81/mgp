/**
 * Copyright 2013 Jesse Anderson
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CardMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  private static Pattern inputPattern = Pattern.compile("(\\d*),(\\d*),(\\d.\\d),(\\d*)");

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String inputLine = value.toString();

    Matcher inputMatch = inputPattern.matcher(inputLine);

    // Use regex to throw out Jacks, Queens, Kings, Aces and Jokers
    if (inputMatch.matches()) {
      // Normalize inconsistent case for card suits

      String movieId = inputMatch.group(2);
      double rating = Double.valueOf(inputMatch.group(3));

      context.write(new Text(movieId), new DoubleWritable(rating));
    }
  }
}