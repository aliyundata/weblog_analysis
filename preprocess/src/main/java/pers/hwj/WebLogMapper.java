package pers.hwj;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * @Author hwj
 * @Date 2020/8/6 14:37
 * @Desc:
 **/
/*
k1  起始偏移量   LongWritable
v1  行文本内容   Text
k2  JAVA Bean   WebLogBean
v2  NULL         NullWritable
LongWritable, Text, Text, NullWritable
 */
/*
1. 行文本拆分，得到各个Bean字段，获取k2
2. 将 k2,v2 写入上下文
** 注意提出的几种类型数据如下 **
1. 不是指定网页跳转过来的请求（可能爬虫）
2. HTTP 状态码 >400 的请求
3. 时间为空的剔除
 */
public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    // 时间格式转换
    public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
    public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
    // 用来存储网站url分类数据
    Set<String> pages = new HashSet<String>();
    Text k = new Text();
    NullWritable v = NullWritable.get();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();
        String[] split = text.split(" ");
        WebLogBean logBean = new WebLogBean();
        // 若
        if(split.length>11) {

//            private boolean valid = true; // 判断数据是否合法

            logBean.setRemote_ip(split[0]);
            logBean.setRemote_user(split[1]);
            String time_local=formatDate(split[3].substring(1));
            if(time_local.equals("")||time_local==null){
                time_local="-invalid_time-";
            }
            logBean.setTime_local(time_local);
            logBean.setRequest(split[6]);
            logBean.setStatus(split[8]);
            logBean.setBody_bytes_sent(split[9]);
            logBean.setHttp_referer(split[10]);
            // 如果 user agent 元素较多，拼接 server agent
            if(split.length>12){
                StringBuilder stringBuilder = new StringBuilder();
                for(int i=11;i<split.length;i++) {
                    stringBuilder.append(split[i]);
                }
                logBean.setHttp_user_agent(stringBuilder.toString());
            }else{
                logBean.setHttp_user_agent(split[11]);
            }
            // 对于明显不合规的数据，创建标记位，进行逻辑删除
            if(Integer.parseInt(logBean.getStatus())>=400){
                logBean.setValid(false);
            }
            if(logBean.getTime_local().equals("-invalid_time-")){
                logBean.setValid(false);
            }
            filtStaticResource(logBean, pages);
        }else{
            logBean=null;
        }
        if (logBean != null) {
            // 过滤js/图片/css等静态资源
            filtStaticResource(logBean, pages);
            /* if (!webLogBean.isValid()) return; */
            k.set(logBean.toString());
            context.write(k, v);
        }
    }
    // 定义时间格式转换
    public static String formatDate(String time_local) {
        try {
            return df2.format(df1.parse(time_local));
        } catch (ParseException e) {
            return null;
        }
    }
    public static void filtStaticResource(WebLogBean bean, Set<String> pages) {
        if (!pages.contains(bean.getRequest())) {
            bean.setValid(false);
        }
    }
}
