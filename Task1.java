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

public class Task1 extends Configured implements Tool{

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new Task1(), args);
    }

    public int run(String[] args) throws Exception{

        String input = args[0];
        String output = args[1];

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Task1.class);

        job.setMapperClass(T1_MAP.class);
        job.setReducerClass(T1_Reduce.class);

        job.setMapOutputKeyClass(IntPairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);

        return 0;
    }

    public static class T1_MAP extends Mapper<Object, Text, IntPairWritable, IntWritable>{

        IntPairWritable edge = new IntPairWritable();
        IntWritable minusOne = new IntWritable(-1);

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntPairWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {

            StringTokenizer st = new StringTokenizer(value.toString());


            int u = Integer.parseInt(st.nextToken());
            int v = Integer.parseInt(st.nextToken());

            // PairWritable객체의 키값으로 설정
            // 여기서 같은것은 포함하지 않았으므로 self-loop 제거 되고
            // (1, 9) (9, 1)이 중복 엣지이니 (작은, 큰) 순으로 emit을 하면 된다.
            if(u != v) {
                if(u < v) edge.set(u, v);
                else edge.set(v, u);

                context.write(edge, minusOne);
            }
        }
    }

    public static class T1_Reduce extends Reducer<IntPairWritable, IntWritable, IntWritable, IntWritable>{

        IntWritable ou = new IntWritable();
        IntWritable ov = new IntWritable();

        @Override
        protected void reduce(IntPairWritable key, Iterable<IntWritable> values,
                              Reducer<IntPairWritable, IntWritable, IntWritable, IntWritable>.Context context)
            throws IOException, InterruptedException{


            // 키가 intpairwritable이므로 키만 emit해주면 된다.
            ou.set(key.u);
            ov.set(key.v);

            context.write(ou, ov);
        }
    }


}
