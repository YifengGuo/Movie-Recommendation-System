package main.version1.java;

/**
 * @author yifengguo
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Build Co-occurrence Matrix by the output of first MapReduce Job
 */
public class CoocurrenceMatrixBuilder {
    public static class CooccurrenceMatrixMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        static IntWritable one = new IntWritable(1);

        /**
         *
         * @param key    line index which is LongWritable
         * @param value  each row of result of first MapReduce Job eg:1	10001:5.0,10002:3.0,10003:2.5
         * @param context
         * @throws InterruptedException
         * @throws IOException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
            // get the 10001:5.0,10002:3.0,10003:2.5 part from input text
            String[] movie_rating = value.toString().trim().split("\t")[1].split(",");  // { 1:10, 2:8...}

            // retrieve the movie id
            String[] movie_id = new String[movie_rating.length];
            /*
             * construct movie_id array {1, 2, 3, 4...}
             */
            for (int i = 0; i < movie_rating.length; i++) {
                movie_id[i] = movie_rating[i].split(":")[0];
            }

            /*
             * for for loop to traverse all combinations between all movies
             * output: 1:1   1
             *         1:2   1
             *         2:1   1
             *         2:2   1
             */
            for (int i = 0; i < movie_id.length; i++) {
                for (int j = 0; j < movie_id.length; j++) {
                    context.write(new Text(movie_id[i] + ":" + movie_id[j]), one);
                }
            }
        }
    }

    public static class CooccurrenceMatrixReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        /**
         * simply merge the result of mapper like reducer of word count
         *                         reducer
         *         key   value     ------>       key         value
         *         1:1   1                       1:1          2
         *         1:2   1                       1:2          1
         *         2:1   1                       2:1          1
         *         2:2   1                       2:2          2
         *         1:1   1
         *         2:2   1
         * @param key
         * @param values
         * @param context
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
                InterruptedException, IOException {
            // Iterator<IntWritable> iterator = values.iterator();
            int count = 0;
            while (values.iterator().hasNext()) {
                count += values.iterator().next().get();
            }

            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(CooccurrenceMatrixMapper.class);
        job.setReducerClass(CooccurrenceMatrixReducer.class);

        job.setJarByClass(CoocurrenceMatrixBuilder.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0])); // output of the first MapReduce job
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
