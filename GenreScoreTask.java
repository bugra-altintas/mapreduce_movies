

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

public class GenreScoreTask {

    public static class GenreScoreMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, FloatWritable> {
        public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException, IOException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            while (itr.hasMoreTokens()) {
                String line = itr.nextToken();
                String[] tokens = line.split("\t");

                String genre = tokens[2];
                String score = tokens[5];

                if(score.equals("score") || score.equals("")) {
                    continue;
                }

                output.collect(new Text(genre), new FloatWritable(Float.parseFloat(score)));

            }

        }

    }


    public static class GenreScoreReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            float sum = 0;
            int count = 0;
            while (values.hasNext()) {
                sum += values.next().get();
                count++;
            }

            if(count > 9){
                float average = sum / count;
                output.collect(key, new FloatWritable(average));
            }


        }


    }
}
