

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.math.*;

public class TotalTask {

    public static class TotalMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, FloatWritable> {

        public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException, IOException {

            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            while (itr.hasMoreTokens()) {
                String line = itr.nextToken();
                String[] tokens = line.split("\t");
                if(tokens.length != 15) {
                    continue;
                }

                String name = "total_runtime";
                String runtime = tokens[14];
                if(runtime.equals("runtime")) {
                    continue;
                }

                output.collect(new Text(name), new FloatWritable(Float.parseFloat(runtime)));

            }

        }

    }

    public static class TotalReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            float sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }

            output.collect(key, new FloatWritable(sum));

        }


    }
}
