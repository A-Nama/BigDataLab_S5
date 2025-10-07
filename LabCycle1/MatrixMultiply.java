import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MatrixMultiply {

    // Hardcoded matrices
    // A is 2x3
    static int[][] A = {
            {1, 2, 3},
            {4, 5, 6}
    };

    // B is 3x2
    static int[][] B = {
            {7, 8},
            {9, 10},
            {11, 12}
    };

    // Mapper
    public static class MatrixMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Dimensions
            int aRows = A.length;
            int aCols = A[0].length;
            int bCols = B[0].length;

            // Emit partial products
            for (int i = 0; i < aRows; i++) {
                for (int k = 0; k < aCols; k++) {
                    for (int j = 0; j < bCols; j++) {
                        int product = A[i][k] * B[k][j];
                        context.write(new Text(i + "," + j), new IntWritable(product));
                    }
                }
            }
        }
    }

    // Reducer
    public static class MatrixReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Multiplication");

        job.setJarByClass(MatrixMultiply.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // First arg = input, second arg = output
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
