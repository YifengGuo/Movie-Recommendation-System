package main.version2.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yifengguo
 */
@SuppressWarnings("Duplicates")
public class RecommendationListGenerator {
    public static class RecommendationMapper extends Mapper<LongWritable, Text, Text, Text> {

        Map<Integer, List<Integer>> watchHistoryMap = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("watchHistory");
            Path path = new Path(filePath);
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line = "";
            while ((line = br.readLine()) != null) {
                String[] tokens = line.trim().split(",");
                int user_id = Integer.parseInt(tokens[0]);
                int movie_id = Integer.parseInt(tokens[1]);
                if (watchHistoryMap.containsKey(user_id)) {
                    watchHistoryMap.get(user_id).add(movie_id);
                } else {
                    List<Integer> list = new ArrayList<>();
                    list.add(movie_id);
                    watchHistoryMap.put(user_id, list);
                }
            }
            br.close();
        }

        /**
         *
         * @param key byte offset
         * @param value user_id:movie_id \t partial_score
         *
         *                  key             value
         * @param context user_id   \t  unwatched_movie_id : score
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().trim().split("\t");
            int user_id = Integer.parseInt(tokens[0].split(":")[0]);
            int movie_id = Integer.parseInt(tokens[0].split(":")[1]);
            String partial_score = tokens[1];
            // filter out watched ones
            if (watchHistoryMap.get(user_id).contains(movie_id)) {
                return;
            }
            context.write(new Text(user_id + ":" + movie_id), new Text(partial_score));
        }
    }

    public static class RecommendationReducer extends Reducer<Text, Text, Text, Text> {
        // movie_id -> movie_title
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
         * sum up partial scores of movies and output as recommendation list for users on unwatched movies
         * @param key user_id : movie_id
         * @param values list of partial_score
         * @param context user_id  \t  movie_title : total_score
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double total_score = 0.0;
            String user_id = key.toString().split(":")[0];
            String movie_title = movieTitleMap.get(Integer.parseInt(key.toString().split(":")[1]));
            for (Text value : values) {
                total_score += Double.parseDouble(value.toString());
            }
            // format total score
            DecimalFormat df = new DecimalFormat("#.00");
            total_score = Double.valueOf(df.format(total_score));
            context.write(new Text(user_id), new Text(movie_title + ":" + total_score));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // conf must set the String and its parameter before initialize Job
        conf.set("watchHistory", args[0]); // src/main/version2/raw_data/user_rating_history.txt
        conf.set("movieTitles", args[1]);  // src/main/version2/raw_data/movie_title.txt

        Job job = Job.getInstance(conf);

        job.setMapperClass(RecommendationMapper.class);
        job.setReducerClass(RecommendationReducer.class);

        job.setJarByClass(RecommendationListGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // src/main/version2/output/matrices_multiplication/part-r-00000
        // output of matrices multiplication
        TextInputFormat.setInputPaths(job, new Path(args[2]));
        TextOutputFormat.setOutputPath(job, new Path(args[3])); // src/main/version2/output/recommendation_list/

        job.waitForCompletion(true);
    }
}
