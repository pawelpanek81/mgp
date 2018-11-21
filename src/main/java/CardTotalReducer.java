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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CardTotalReducer extends Reducer<Text, DoubleWritable, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,
      InterruptedException {
    double sum = 0;
    int counter = 0;

    // Go through all values to sum up card values for a card suit
    for (DoubleWritable value : values) {
      sum += value.get();
      counter += 1;
    }

    String output = String.valueOf(Math.round(sum / counter * 100.0) / 100.0) + ", " + String.valueOf(counter);

    context.write(key, new Text(output));
  }
}
