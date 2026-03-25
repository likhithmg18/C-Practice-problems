package loganalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class loganalysis {

	public static class MapperClass
    extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split(" ");

        String ip = fields[0];        // IP address
        String resource = fields[6]; // URL
        String status = fields[8];   // status code

        context.write(new Text("IP_" + ip), new IntWritable(1));
        context.write(new Text("STATUS_" + status), new IntWritable(1));
        context.write(new Text("RES_" + resource), new IntWritable(1));
    }
}

public static class ReducerClass
    extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "hdfs log analysis");

    job.setJarByClass(loganalysis.class);
    job.setMapperClass(MapperClass.class);
    job.setReducerClass(ReducerClass.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
