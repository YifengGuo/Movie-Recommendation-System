package main.version1.java;

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
public class DataDividerByUser {
    public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        /**
         *
         * @param key  line index
         * @param value user_id, movie_id, rating
         * @param context key: user_id      value: (movie_id : rating)
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] user_movie_rating = value.toString().trim().split(",");
            int user_id = Integer.parseInt(user_movie_rating[0]);
            String movie_id = user_movie_rating[1];
            String rating = user_movie_rating[2];

            context.write(new IntWritable(user_id), new Text(movie_id + ":" + rating));
        }
    }

    public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        /**
         *
         * @param key  user_id
         * @param values  all (movie_id : rating) of this user
         * @param context  key: user_id      value: list of (movie_id : rating)
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            StringBuilder sb = new StringBuilder();  // append all (movie_id : rating) for this user
            while (values.iterator().hasNext()) {
                sb.append("," + values.iterator().next());
            }

            // sb: ,movie1:2,movie2:3.5,movie5:7
            // replace first extra ","
            context.write(key, new Text(sb.toString().replaceFirst(",", "")));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DataDividerReducer.class);

        job.setJarByClass(DataDividerByUser.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // the output format of mapper and reducer are the same
        // so set once
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0])); // src/main/version1/raw_data/user_rating_history.txt
        TextOutputFormat.setOutputPath(job, new Path(args[1])); // src/main/version1/output/first_mapreduce

        job.waitForCompletion(true);
    }
}
