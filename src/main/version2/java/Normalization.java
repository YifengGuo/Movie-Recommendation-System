package main.version2.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yifengguo
 */

/**
 * read input from the output of CoccurrenceMatrixGenerator
 * cache each movie with all movies relation in a HashMap
 * for each movie in the map, calculate and write the normalized relation between it and input key
 */
public class Normalization {
    public static class NormalizationMapper extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * input:  output of CoccurrenceMatrixGenerator
         * @param key  byte offset
         * @param value movieA:movieB \t relation
         * @param context  key       value
         *                movieA \t movieB:relation
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String movieA = tokens[0].split(":")[0];
            String movieB = tokens[0].split(":")[1];
            String relation = tokens[1];
            context.write(new Text(movieA), new Text(movieB + ":" + relation));
        }
    }

    public static class NormalizationReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * input of reducer:   movieA \t {movieA:relation1, movieB:relation2,...}
         * @param key movieA
         * @param values {movieA:relation1, movieB:relation2,...}
         * @param context  key:        movieB  (movies on the column direction)
         *                 value:      movieA=normalized_relation  (movies on the row direction)
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key: movie, value: sum of relation for current movie and key movieA
            Map<String, Integer> relationSumMap = new HashMap<>();
            int sum = 0;
            // cache each movie related to key movieA's relation sum in the map
            while (values.iterator().hasNext()) {
                // values: {movieA:relaition1, movieB:relation2...movieX:relationX}
                String[] movie_relation = values.iterator().next().toString().trim().split(":");
                String movie_id = movie_relation[0];
                int relation = Integer.parseInt(movie_relation[1]);
                // in fact this process is to add one row relation sum in the cooccurrence matrix
                sum += relation;
                relationSumMap.put(movie_id, relation); // put the movie on the column with sum relation
                                                        // because so far the cooccurrence_matrix is still symmetrical
            }

            for (Map.Entry<String, Integer> entry : relationSumMap.entrySet()) {
                String outputKey = entry.getKey();  // output key: movieB id (movies on the column ->)
                // outputValue movieA=normalized_relation  (movieA represents movies on the row \)
                // In this way, each entry in the normalized coocurrence matrix can be covered and written as well
                String outputValue = key.toString() + "=" + (double)entry.getValue() / sum;
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(NormalizationMapper.class);
        job.setReducerClass(NormalizationReducer.class);

        job.setJarByClass(Normalization.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0])); // src/main/version2/output/cooccurrence_matrix_generator
                                                               // /part-r-00000
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        // src/main/version2/output/normalization/

        job.waitForCompletion(true);
    }
}
