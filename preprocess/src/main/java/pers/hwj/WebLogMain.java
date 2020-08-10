package pers.hwj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * @Author hwj
 * @Date 2020/8/6 14:29
 * @Desc: 处理原始日志，过滤出真实pv请求 转换时间格式 对缺失字段填充默认值 \
 * 对记录标记valid和invalid
 **/
/*
k1				v1
起始偏移量	该行内容
k2				v2
行内容		    null
 */
public class WebLogMain {
    // 将这个描述好的对象提交给集群去运行
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //  Configuration 封装了对应客户端或服务器的配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WebLogMain.class);

        // 指定 Map 阶段的处理方式
        job.setMapperClass(WebLogMapper.class);

        // 指定 reduce 阶段的处理方式
        job.setNumReduceTasks(0);

        // 指定 Map 阶段键值对输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 指定 reduce 阶段输出到文件的键值对类型
//        FileInputFormat.setInputPaths(job,new Path("file:///G:\\input"));
        FileInputFormat.setInputPaths(job,new Path("E:\\Big_Data_Files\\企业级网站流量运营分析系统开发实战\\网站流日志分析资料\\day2资料\\代码\\数据预处理数据\\weblog\\input"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\Big_Data_Files\\opt\\"));

        // 向 yarn 集群提交这个 job
        boolean res=job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
