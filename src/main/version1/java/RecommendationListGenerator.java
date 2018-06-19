package main.version1.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by guoyifeng on 6/19/18.
 */

public class RecommendationListGenerator {
    /**
     * 1. Filter out users's watched movies. We will not recommend users the film they already movies.
     * 2. Match movie_id to movie_name to make recommendation list more human-readable
     */
    public static class RecommendationListGeneratorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        /**
         * key : user_id      value : watched movie list
         * {user_id : {movie_1, movie3, ...}}
         */
        Map<Integer, List<Integer>> watchHistoryMap = new HashMap<>();

        /**
         * cache users and their watched movie in the memory
         * @param context
         * @throws IOException
         */
        @Override
        public void setup(Context context) throws IOException {
            // read movie watching history
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("watchHistory"); // Get the watched history file (user_rating_history.txt)
            // the path name is set in main()
            Path path = new Path(filePath);
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path))); // get the data from HDFS
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.trim().split(",");
                int user_id = Integer.parseInt(tokens[0]);
                int movie_id = Integer.parseInt(tokens[1]);

                if (watchHistoryMap.containsKey(user_id)) {
                    watchHistoryMap.get(user_id).add(movie_id);
                } else {
                    List<Integer> watchedMovieList = new ArrayList<>();
                    watchedMovieList.add(movie_id);
                    watchHistoryMap.put(user_id, watchedMovieList);
                }
            }
            br.close();
        }

        /**
         * filter out watched movies of the input file (output of MatricesMultiplication reducer)
         *          input value  -->   user_id \t  {movie_id : total_score}
         * @param key  line index
         * @param value   user_id \t  {movie_id : total_score}
         * @param context data without watched ones
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().trim().split("\t");
            int user_id = Integer.parseInt(tokens[0]);
            int movie_id = Integer.parseInt(tokens[1].split(":")[0]);
            double total_score = Double.parseDouble(tokens[1].split(":")[1]);
            // filter out watched movies
            if (watchHistoryMap.get(user_id).contains(movie_id)) {
                return;
            }
            context.write(new IntWritable(user_id), new Text(movie_id + ":" + total_score));
        }
    }

    /**
     * match unwatched movie_id to their real movie name
     * get the movie names in setup()
     */
    public static class RecommendationListGeneratorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        // Map<movie_id, movie_title>
        Map<Integer, String> movieTitleMap = new HashMap<>();
        @Override
        public void setup(Context context) throws IOException {
            // store data in SQL in practical job
            // read movie titles from the file
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("movieTitles");
            Path path = new Path(filePath);
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;

            while ((line = br.readLine()) != null) {
                // input format :    movie_id,movie_name
                String[] tokens = line.trim().split(",");
                int movie_id = Integer.parseInt(tokens[0]);
                String movie_name = tokens[1];
                movieTitleMap.put(movie_id, movie_name);
            }
            br.close();
        }

        /**
         *
         * @param key   user_id
         * @param values   movie_id : total_score
         * @param context  user_id \t movie_name : total_score
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            while (values.iterator().hasNext()) {
                String[] tokens = values.iterator().next().toString().split(":");
                int movie_id = Integer.parseInt(tokens[0]);
                double total_score = Double.parseDouble(tokens[1]);
                String movie_title = movieTitleMap.get(movie_id);
                context.write(key, new Text(movie_title + ":" + total_score));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("watchHistory", args[0]); // user_rating_history.txt
        conf.set("movieTitles", args[1]);  // movie_title.txt

        Job job = Job.getInstance(conf);

        job.setMapperClass(RecommendationListGeneratorMapper.class);
        job.setReducerClass(RecommendationListGeneratorReducer.class);

        job.setJarByClass(RecommendationListGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[2]));  // output of MatricesMultiplication reducer
        TextOutputFormat.setOutputPath(job, new Path(args[3])); // output of this MR job reducer
                                                                // src/main/version1/output/recommendation_list

        job.waitForCompletion(true);
    }
}
