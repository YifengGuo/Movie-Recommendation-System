package main.version2.java;

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
 * @author yifengguo
 */
public class CooccurrenceMatrixGenerator {
    public static class CooccurrenceMatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        static final IntWritable one = new IntWritable(1);
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
            String[] tokens = value.toString().trim().split("\t");

            // retrieve the movie id
            String[] movies = new String[tokens[1].split(",").length];
            /*
             * construct movie_id array {1, 2, 3, 4...}
             */
            for (int i = 0; i < movies.length; i++) {
                movies[i] = tokens[1].split(",")[i].split(":")[0];
            }

            /*
             * for for loop to traverse all combinations between all movies
             * output: 1:1   1
             *         1:2   1
             *         2:1   1
             *         2:2   1
             */
            for (int i = 0; i < movies.length; i++) {
                for (int j = 0; j < movies.length; j++) {
                    context.write(new Text(movies[i] + ":" + movies[j]), one);
                }
            }
        }
    }

    public static class CooccurrenceMatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws InterruptedException, IOException {
            int total = 0;
            while (values.iterator().hasNext()) {
                total += values.iterator().next().get();
            }
            context.write(key, new IntWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(CooccurrenceMatrixGeneratorMapper.class);
        job.setReducerClass(CooccurrenceMatrixGeneratorReducer.class);

        job.setJarByClass(CooccurrenceMatrixGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0])); // src/main/version2/output/data_divider/part-r-00000
                                                               // output of the first mapreduce job
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
                                                              // src/main/version2/output/cooccurrence_matrix_generator/

        job.waitForCompletion(true);
    }
}
