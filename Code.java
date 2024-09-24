import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class SalesAnalysis {

    // First Job Mapper
    public static class SalesMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable totalSalesAmount = new LongWritable();
        private Text retailerCity = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().startsWith("Retailer")) {
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length == 6) {
                try {
                    String retailer = fields[0].trim();
                    String city = fields[2].trim();
                    int pricePerUnit = Integer.parseInt(fields[4].replace("$", "").replace(",", "").trim());
                    int unitsSold = Integer.parseInt(fields[5].replace(",", "").trim());
                    long salesAmount = (long) pricePerUnit * unitsSold;

                    retailerCity.set(retailer + ", " + city);
                    totalSalesAmount.set(salesAmount);

                    context.write(retailerCity, totalSalesAmount);
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing record: " + value.toString() + " " + e.getMessage());
                }
            }
        }
    }

    // First Job Reducer
    public static class SalesReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Second Job Mapper
    public static class SortMapper extends Mapper<Object, Text, LongWritable, Text> {
        private Text retailerCity = new Text();
        private LongWritable salesAmount = new LongWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length == 2) {
                retailerCity.set(fields[0]);
                salesAmount.set(Long.parseLong(fields[1]));

                // The negative sign makes the LongWritable value descending
                context.write(new LongWritable(-salesAmount.get()), retailerCity);
            }
        }
    }

    // Second Job Reducer
    public static class SortReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                // Negate the key back to its original positive value
                context.write(val, new LongWritable(-key.get()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Configuration and First Job
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "sales sum");

        job1.setJarByClass(SalesAnalysis.class);
        job1.setMapperClass(SalesMapper.class);
        job1.setCombinerClass(SalesReducer.class);
        job1.setReducerClass(SalesReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

        boolean success = job1.waitForCompletion(true);
        if (!success) {
            System.exit(1);
        }

        // Configuration and Second Job
        Job job2 = Job.getInstance(conf, "sort by sales amount");

        job2.setJarByClass(SalesAnalysis.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);

        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
