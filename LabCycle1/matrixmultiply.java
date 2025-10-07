import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        /*
         * Input line format:
         * For A: A,i,j,value
         * For B: B,j,k,value
         */

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            String matrixName = parts[0];
            if (matrixName.equals("A")) {
                String i = parts[1];
                String j = parts[2];
                String val = parts[3];
                // Emit key = i,k (k unknown here, so we emit for all k?)
                // Instead, emit with key = i,j with marker 'A'
                // Actually better to emit key as i,j with marker and value

                // To join on j, emit key as j with info about matrix and i/k indices.

                // Actually, standard approach:

                // Emit key = j with value "A,i,val"

                context.write(new Text(j), new Text("A," + i + "," + val));
            } else { // matrix B
                String j = parts[1];
                String k = parts[2];
                String val = parts[3];
                // Emit key = j with value "B,k,val"
                context.write(new Text(j), new Text("B," + k + "," + val));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key = j
            // values contain all A(i,j) and B(j,k)
            ArrayList<Element> listA = new ArrayList<>();
            ArrayList<Element> listB = new ArrayList<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts[0].equals("A")) {
                    int i = Integer.parseInt(parts[1]);
                    int v = Integer.parseInt(parts[2]);
                    listA.add(new Element(i, v));
                } else if (parts[0].equals("B")) {
                    int k = Integer.parseInt(parts[1]);
                    int v = Integer.parseInt(parts[2]);
                    listB.add(new Element(k, v));
                }
            }

            // Now multiply elements from listA and listB with matching j to contribute to C(i,k)
            for (Element a : listA) {
                for (Element b : listB) {
                    int i = a.index;
                    int k = b.index;
                    int product = a.value * b.value;
                    // Emit partial product with key (i,k)
                    context.write(new Text(i + "," + k), new IntWritable(product));
                }
            }
        }

        static class Element {
            int index;
            int value;

            public Element(int index, int value) {
                this.index = index;
                this.value = value;
            }
        }
    }

    public static class SumReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "matrix multiplication partial product");
        job1.setJarByClass(MatrixMultiplication.class);
        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path tempOutput = new Path("temp_output");
        FileOutputFormat.setOutputPath(job1, tempOutput);

        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "matrix multiplication sum");
        job2.setJarByClass(MatrixMultiplication.class);
        job2.setMapperClass(Mapper.class); // identity mapper
        job2.setReducerClass(SumReduce.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, tempOutput);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
