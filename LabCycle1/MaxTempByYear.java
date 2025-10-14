import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTempByYear {

    public static class TempMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text year = new Text();
        private DoubleWritable temp = new DoubleWritable();
        private boolean isHeader = true;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (isHeader) {
                isHeader = false; // skip header line
                return;
            }

            String[] fields = line.split(",", -1);
            if (fields.length >= 2) {
                String date = fields[0].trim();
                String tempStr = fields[1].trim();

                if (!tempStr.isEmpty()) {
                    try {
                        String yr = date.split("-")[0];
                        double t = Double.parseDouble(tempStr);
                        year.set(yr);
                        temp.set(t);
                        context.write(year, temp);
                    } catch (Exception e) {
                        // skip malformed lines
                    }
                }
            }
        }
    }

    public static class MaxReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double maxTemp = Double.MIN_VALUE;
            for (DoubleWritable val : values) {
                maxTemp = Math.max(maxTemp, val.get());
            }
            context.write(key, new DoubleWritable(maxTemp));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Temp By Year");
        job.setJarByClass(MaxTempByYear.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(MaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
