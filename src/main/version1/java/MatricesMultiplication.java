package main.version1.java;

/**
 * Created by guoyifeng
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * To do the multiplication on Co-occurrence Matrix and Rating Matrix<br>
 * Because the Mapper each time read only one file, so we choose to cache the Co-occurrence Matrix
 * in the memory by a HashMap.<br>
 * The relation of the Co-occurrence Matrix is maintained by a helper class {@link MovieRelation}
 */
public class MatricesMultiplication {
    public static class MatricesMultiplicationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        /**
         * To initialize and store the Co-occurrence Matrix in a HashMap and
         * will be used in multiplication during mapping process by the Mapper
         */

        /**
         *  store the Co-occurrence Matrix in HashMap maintained by a helper class
         *  {movie_id : List[movie1:movie2 -> relation]}
         *  e.g {movie1: movie1:movie1 -> 4, movie1:movie2 -> 6, movie1:movie3 -> 8}
         */
        Map<Integer, List<MovieRelation>> movieRelationMap = new HashMap<>();

        /**
         * Used in calculating the denominator when normalizing the Co-occurrence Matrix
         */
        Map<Integer, Integer> denominatorMap = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("coOccurrencePath"); // Get the Co-occurrence Matrix from output of MR job 2
                                                            // the path name is set in Drive.java
            Path path = new Path(filePath);
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path))); // get the data from HDFS

            // read Co-occurrence Matrix
            // format:      movie_id:movie_id \t relation
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.toString().trim().split("\t");
                String[] movies = tokens[0].split(":");

                int movie1 = Integer.parseInt(movies[0]);
                int movie2 = Integer.parseInt(movies[1]);
                int relation = Integer.parseInt(tokens[1]);

                if (movieRelationMap.containsKey(movie1)) {
                    movieRelationMap.get(movie1).add(new MovieRelation(movie1, movie2, relation));
                } else {
                    List<MovieRelation> list = new ArrayList<>();
                    list.add(new MovieRelation(movie1, movie2, relation));
                    movieRelationMap.put(movie1, list);
                }
            }
            br.close();

            // initialize denominatorMap
            for (Map.Entry<Integer, List<MovieRelation>> entry : movieRelationMap.entrySet()) {
                int sum = 0;
                for (MovieRelation relation : entry.getValue()) {
                    sum += relation.getRelation();
                }
                denominatorMap.put(entry.getKey(), sum);
            }
        }


        @Override
        public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
            /**
             *  input (User Rating Matrix): user,movie_id,rating
             *
             *               key           value
             *  output: user:movie2       score (score is calculated by multiplication on corresponding entries)
             *
             *  Co-occurrence Matrix map's key is movie1
             *  so the output is user's rating on movie2!!!
             */
            String[] tokens = value.toString().trim().split(",");
            int user_id = Integer.parseInt(tokens[0]);
            int movie_id = Integer.parseInt(tokens[1]);  // it is also the key of corresponding entry in HashMap
            double rating = Double.parseDouble(tokens[2]);

            for (MovieRelation relation : movieRelationMap.get(movie_id)) {
                // Multiplication on each single entry of two matrices
                double score = rating * relation.getRelation();
                // Normalization
                score /= denominatorMap.get(relation.getMovie2()); // denominator is sum on row, so should get movie2
                // confirm the precision for the score
                DecimalFormat df = new DecimalFormat("#.00");
                score = Double.valueOf(df.format(score));

                context.write(new Text(user_id + ":" + relation.getMovie2()), new DoubleWritable(score));
            }
        }
    }

    public static class MatricesMultiplicationReducer extends Reducer<Text, DoubleWritable, IntWritable, Text> {
        /**
         *
         * @param key    user_id:movie_id
         * @param values {1/6,2/3,2/11...}
         * @param context user_id \t {movie_id : total_score}
         * @throws InterruptedException
         * @throws IOException
         */
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws InterruptedException,
                IOException {
            double total = 0.0;
            while (values.iterator().hasNext()) {
                total += values.iterator().next().get();
            }
            String[] tokens = key.toString().trim().split(":");
            int user_id = Integer.parseInt(tokens[0]);
            int movie_id = Integer.parseInt(tokens[1]);
            context.write(new IntWritable(user_id), new Text(movie_id + ":" + total));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("coOccurrencePath", args[0]); // args[0] src/main/version1/output/second_mapreduce/part-r-00000

        Job job = Job.getInstance(conf);
        job.setMapperClass(MatricesMultiplicationMapper.class);
        job.setReducerClass(MatricesMultiplicationReducer.class);

        job.setJarByClass(MatricesMultiplication.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // because the output key and value of Mapper and Reducer are different
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[1]));
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
