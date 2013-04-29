package utils;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;

public class HadoopUtils {
  public static void printConfiguration(Configuration conf){
    for(Entry<String, String> entry: conf){
      //if(entry.getKey().matches("(mapreduce\\.jobtracker\\.address|fs\\.default\\.name|mapreduce\\.framework\\.name)"))
        System.out.printf("%s\t%s\n", entry.getKey(), entry.getValue());
    }
  }
  
//  public static void printCounters(Counters counters){
//    while(counters..iterator().hasNext()){
//      Counter counter = counters.iterator().next();
//      System.out.printf("%s\t%s\n", counter.getName(); counter.)
//    }
//  }
}
