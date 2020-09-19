package bigdata;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class Task2 extends Configured implements Tool{

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new Task2(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        String input = args[0];
        String output = args[1];

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Task2.class);

        job.setMapperClass(T2_MAP.class);
        job.setReducerClass(T2_Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);

        return 0;
    }

    public static class T2_MAP extends Mapper <Object, Text, IntWritable, IntWritable>{

        IntWritable one = new IntWritable(1);

        IntWritable ou = new IntWritable();
        IntWritable ov = new IntWritable();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {

            StringTokenizer st = new StringTokenizer(value.toString());
            // 키랑 밸류 둘다 1로 emit 해줘야함
            ou.set(Integer.parseInt(st.nextToken()));
            ov.set(Integer.parseInt(st.nextToken()));
            context.write(ou, one);
            context.write(ov, one);
        }
    }

    public static class T2_Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        IntWritable degree = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
                              Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable v: values) sum+= v.get();
            degree.set(sum);

            context.write(key, degree);
        }
    }
}
