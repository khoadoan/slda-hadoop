package cc.slda;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;


import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.math.LogMath;

public class TermCombiner extends 
    Reducer<PairOfInts, DoubleWritable, PairOfInts, DoubleWritable> {
  private DoubleWritable outputValue = new DoubleWritable();

  public void reduce(PairOfInts key, Iterator<DoubleWritable> values,
      Context context) throws IOException, InterruptedException {
    double sum = values.next().get();
    if (key.getLeftElement() <= 0) {
      // this is not a phi value
      while (values.hasNext()) {
        sum += values.next().get();
      }
    } else {
      // this is a phi value
      while (values.hasNext()) {
        sum = LogMath.add(sum, values.next().get());
      }
    }
    outputValue.set(sum);
    context.write(key, outputValue);
  }
}