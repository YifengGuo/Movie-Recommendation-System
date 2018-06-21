package main.version2.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yifengguo
 */
public class MatricesMultiplication {
    public static class NormalizedCooccurrenceMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        /**
         *
         * @param key byte offset
         * @param value output of Normalization   movieB \t movieA=normalized_relation
         * @param context output value without any change
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value format  -->    movieB \t movieA=normailized_relation
            String[] tokens = value.toString().split("\t");
            // write out value directly without any change for this mapper
            context.write(new Text(tokens[0]), new Text(tokens[1]));
        }
    }

    public static class RatingHistoryMapper extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * To make sure two mappers share the same key to do the multiplication, output key of this mapper is movie_id
         * @param key byte offset
         * @param value line of user_rating_history.txt
         * @param context key: movie_id  value: user_id:rating
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().trim().split(",");
            context.write(new Text(tokens[1]), new Text(tokens[0] + ":" + tokens[2]));
        }
    }

    public static class MatriceMultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        /**
         *
         * @param key movieB (movies on the column direction)
         * @param values two kinds of values
         *               one from NormalizedCooccurrenceMatrixMapper   {movieX=normalized_relation, ...}
         *               one from RatingHistoryMapper                  {user_id:rating, ...}
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // to store each entry in normalized cooccurrence matrix
            // key: movie_id   value: normalized relation
            Map<String, Double> normalizedCoocurrenceMatrixMap = new HashMap<>();

            // to store each entry in user rating history
            // key: user_id     value: user rating on watched movie
            Map<String, Double> ratingHistoryMap = new HashMap<>();

            for (Text value : values) {
                if (value.toString().contains("=")) {
                    String[] movie_relation = value.toString().trim().split("=");
                    normalizedCoocurrenceMatrixMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
                } else {
                    String[] user_rating = value.toString().trim().split(":");
                    ratingHistoryMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
                }
            }

            /**
             * This for for loop is to multiply each corresponding column of normalized cooccurrence matrix
             * with user rating matrix which share the same MovieA of this reducer
             * during reduce process, input from cooccurrence mapper is a column in the matrix (mA=r1,
             *                                                                                  mB=r2,
             *                                                                                  mC=r3)
             * and key count of normalizedCoocurrenceMatrixMap is movies total count
             * meanwhile, for each user_id for simply each entry of ratingHistoryMap, there is only one value
             *
             * during the shuffle process, all values with same key will be put to one reducer
             * so in this demo, two matrices data are having identical key (M1)
             *
             *      M1    M2                        userA rating
             *  M1 2/6    2/6                 M1        3
             *  M2 2/11   4/11                M2        7
             *  M3 1/6    2/6                 M3        8
             *  M4 1/5    2/5                 M4        0
             *  M5 0      1/3                 M5        0
             *
             *  so for every entry on column M1, it should be multiplied with userA's mapping 3
             *     for every entry on column M2, it should be multiplied with UserA's mapping 7
             *
             *     and all the entries under M1 column are one of input of this reducer,
             *     it's mapping key of row movie is in the normalizedCoocurrenceMatrixMap
             *
             *     This is for each entry of two matrices multiplication
             *     We need one more MapReduce Job to sum up all these partial result and
             *     generate the recommendation list for users
             *
             */
            for (Map.Entry<String, Double> entry : normalizedCoocurrenceMatrixMap.entrySet()) {
                String movie = entry.getKey();
                double relation = entry.getValue();

                for (Map.Entry<String, Double> element : ratingHistoryMap.entrySet()) {
                    String user_id = element.getKey();
                    double rating = element.getValue();
                    context.write(new Text(user_id + ":" + movie), new DoubleWritable(rating * relation));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MatricesMultiplication.class);


        // add two mappers by ChainMapper
        ChainMapper.addMapper(job, NormalizedCooccurrenceMatrixMapper.class,
                LongWritable.class, Text.class, Text.class, Text.class, conf);

        ChainMapper.addMapper(job, RatingHistoryMapper.class, Text.class,
                Text.class, Text.class, Text.class, conf);

        job.setMapperClass(NormalizedCooccurrenceMatrixMapper.class);
        job.setMapperClass(RatingHistoryMapper.class);
        job.setReducerClass(MatriceMultiplicationReducer.class);


        // mappers' output format are not the same as reducer's
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // method to add multiple input data source, declare input format in the method
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,
                NormalizedCooccurrenceMatrixMapper.class); // src/main/version2/output/normalization/part-r-00000
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,
                RatingHistoryMapper.class); // src/main/version2/raw_data/user_rating_history.txt

        TextOutputFormat.setOutputPath(job, new Path(args[2])); // src/main/version2/output/matrices_multiplication/

        job.waitForCompletion(true);
    }
}
