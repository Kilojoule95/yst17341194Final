import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import javax.naming.Context;

public class InvertedIndex {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text keyGet = new Text(); // 存储单词和URL组合
        private Text valueGet = new Text(); // 存储词频
        private String pattern = "[^a-zA-Z0-9-]";//符号
        @Override
        // 实现map函数
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();// 将每一行转化为一个String
            line = line.replaceAll(pattern, " ");// 将标点符号等字符用空格替换，这样仅剩单词
            FileSplit split = (FileSplit) context.getInputSplit();//获取键值对的FileSplit对象
            StringTokenizer breakup = new StringTokenizer(line);//准备分隔字符串
            while (breakup.hasMoreTokens()) {//若还有分隔符则继续
                int Index = split.getPath().toString().indexOf("input");//获取文件路径
                keyGet.set(breakup.nextToken() + ":" + "file location:" + split.getPath().toString().substring(Index));//获取key
                valueGet.set("1");//将value初始化为1
                context.write(keyGet, valueGet);//返回键值对
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
        private Text value1 = new Text();
        // 实现reducer1函数，赋给词频
        public void reduce(Text key, Iterable<Text> v, Context context) throws IOException, InterruptedException {
            int sum = 0;//重置计数器
            for (Text value : v) {// 统计词频
                sum += Integer.parseInt(value.toString());
            }
            int Index = key.toString().indexOf(":");
            value1.set(key.toString().substring(Index + 1) + ":" + sum);// 将value重置为URL和词频
            key.set(key.toString().substring(0, Index));//将key重置为单词
            context.write(key, value1);
        }
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
        private Text value2 = new Text();
        // 实现reducer2函数，赋给文件路径
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 生成文档列表
            StringBuilder fl = new StringBuilder();
            for (Text value : values) {
                fl.append(value.toString()).append(";");
            }
            value2.set(fl.toString());
            context.write(key, value2);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();//创建配置对象
        Job job = new Job(conf, "Inverted Index");//创建Job对象
        job.setJarByClass(InvertedIndex.class);//设置运行Job的类
        job.setMapperClass(Map.class);//设置Mapper类
        job.setCombinerClass(Reducer1.class);//设置第一个Reducer类
        job.setReducerClass(Reduce2.class);//设置第二个Reducer类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);//设置Map输出的键值对
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);//设置Reduce输出的键值对
        FileInputFormat.addInputPath(job, new Path(args[0]));//设置输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//设置输出路径
        boolean b = job.waitForCompletion(true);//
        if(!b) {
            System.out.println("Wordcount task fail!");
        }

    }
}
