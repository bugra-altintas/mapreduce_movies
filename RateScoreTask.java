

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

public class RateScoreTask {

    public static class RateScoreMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, FloatWritable> {
        public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException, IOException {

            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");


            while (itr.hasMoreTokens()) {
                String line = itr.nextToken();
                String[] tokens = line.split("\t");

                String type = tokens[1];
                String votes = tokens[6];

                //types are G, PG, PG-13 and R

                if(type.equals("G") || type.equals("PG") || type.equals("PG-13") || type.equals("R")) {
                    //if votes is not empty
                    if(votes.equals("")) {
                        continue;
                    }
                    output.collect(new Text(type), new FloatWritable(Float.parseFloat(votes)));
                }
            }

        }

    }

    public static class RateScoreReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            float sum = 0;
            int count = 0;
            while (values.hasNext()) {
                sum += values.next().get();
                count++;
            }

            float average = sum / count;

            output.collect(key, new FloatWritable(average));

        }


    }
}
