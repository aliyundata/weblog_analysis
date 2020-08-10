package pers.hwj;

import jdk.nashorn.internal.objects.annotations.Constructor;

import org.apache.hadoop.io.Writable;

import java.beans.ConstructorProperties;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.text.ParseException;

/**
 * @Author hwj
 * @Date 2020/8/6 14:32
 * @Desc: 根据网站流量日志创建对应的私有属性
 **/
/*
对于明显不合规的数据，创建标记位，进行逻辑删除
该bean是需要序列化操作的，要继承 Writable
主要步骤：
1. 属性定义
2. set get 方法
3. toString
4. 序列化及反序列化
 */
public class WebLogBean implements Writable {
    private boolean valid = true; // 判断数据是否合法
    private String remote_ip; // 记录客户端的IP地址
    private String remote_user; // 客户端用户名称
    private String time_local; // 记录访客时间与时区
    private String request; // 访问请求方式
    private String status; // 记录请求状态
    private String body_bytes_sent; // 记录发送给客户端文件主体内容大小
    private String http_referer; // 记录是从什么页面链接访问过来的
    private String http_user_agent; // 记录客户浏览器的详细信息

    public void set(boolean valid,String remote_ip, String remote_user, String time_local, String request, String status, String body_bytes_sent, String http_referer, String http_user_agent) {
        this.valid = valid;
        this.remote_ip = remote_ip;
        this.remote_user = remote_user;
        this.time_local = time_local;
        this.request = request;
        this.status = status;
        this.body_bytes_sent = body_bytes_sent;
        this.http_referer = http_referer;
        this.http_user_agent = http_user_agent;
    }
    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public String getRemote_ip() {
        return remote_ip;
    }

    public void setRemote_ip(String remote_ip) {
        this.remote_ip = remote_ip;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(valid);
        // \001是hive的默认分隔符，对后面的数据处理来说很方便
        stringBuilder.append("\001").append(remote_ip);
        stringBuilder.append("\001").append(remote_user);
        stringBuilder.append("\001").append(time_local);
        stringBuilder.append("\001").append(request);
        stringBuilder.append("\001").append(status);
        stringBuilder.append("\001").append(body_bytes_sent);
        stringBuilder.append("\001").append(http_referer);
        stringBuilder.append("\001").append(http_user_agent);
        return stringBuilder.toString();
    }

// 序列化方法

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(valid);
        out.writeUTF(null==remote_ip?"":remote_ip);
        out.writeUTF(null==remote_user?"":remote_user);
        out.writeUTF(null==time_local?"":time_local);
        out.writeUTF(null==request?"":request);
        out.writeUTF(null==status?"":status);
        out.writeUTF(null==body_bytes_sent?"":body_bytes_sent);
        out.writeUTF(null==http_referer?"":http_referer);
        out.writeUTF(null==http_user_agent?"":http_user_agent);
    }

    // 反序列化方法
    @Override
    public void readFields(DataInput in) throws IOException {
        this.valid=in.readBoolean();
        this.remote_ip=in.readUTF();
        this.remote_user=in.readUTF();
        this.time_local=in.readUTF();
        this.request=in.readUTF();
        this.status=in.readUTF();
        this.body_bytes_sent=in.readUTF();
        this.http_referer=in.readUTF();
        this.http_user_agent=in.readUTF();
    }


}

