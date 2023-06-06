
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class Hw3 {
    public static void main(String[] args) throws IOException {

        String input_path = args[1].replace(".csv", ".tsv");

        // create job
        JobClient client = new JobClient();

        JobConf job_conf = new JobConf(Hw3.class);

        System.out.println("Starting the job");

        if(args[0].equals("total")){
            job_conf.setJobName("Total");

            job_conf.setMapperClass(TotalTask.TotalMapper.class);
            job_conf.setReducerClass(TotalTask.TotalReducer.class);

            job_conf.setOutputKeyClass(Text.class);
            job_conf.setOutputValueClass(FloatWritable.class);

        } else if (args[0].equals("average")) {
            job_conf.setJobName("Average");

            job_conf.setMapperClass(TotalTask.TotalMapper.class);
            job_conf.setReducerClass(AverageTask.AverageReducer.class);

            job_conf.setOutputKeyClass(Text.class);
            job_conf.setOutputValueClass(FloatWritable.class);


        } else if (args[0].equals("employment")) {
            job_conf.setJobName("Employment");

            job_conf.setMapperClass(EmploymentTask.EmploymentMapper.class);
            job_conf.setReducerClass(EmploymentTask.EmploymentReducer.class);
            
            job_conf.setOutputKeyClass(Text.class);
            job_conf.setOutputValueClass(IntWritable.class);


        } else if (args[0].equals("ratescore")){
            job_conf.setJobName("RateScore");

            job_conf.setMapperClass(RateScoreTask.RateScoreMapper.class);
            job_conf.setReducerClass(RateScoreTask.RateScoreReducer.class);

            job_conf.setOutputKeyClass(Text.class);
            job_conf.setOutputValueClass(FloatWritable.class);



        } else if (args[0].equals("genrescore")){
            job_conf.setJobName("GenreScore");

            job_conf.setMapperClass(GenreScoreTask.GenreScoreMapper.class);
            job_conf.setReducerClass(GenreScoreTask.GenreScoreReducer.class);

            job_conf.setOutputKeyClass(Text.class);
            job_conf.setOutputValueClass(FloatWritable.class);

        }
        else {
            System.out.println("Invalid argument");
            System.exit(0);
        }

        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf, new Path(input_path));
        FileOutputFormat.setOutputPath(job_conf, new Path(args[2]));

        client.setConf(job_conf);

        try {
            JobClient.runJob(job_conf);
            System.out.println("Job completed successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
