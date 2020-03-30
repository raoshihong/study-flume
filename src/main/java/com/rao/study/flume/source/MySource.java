package com.rao.study.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    private String prefix;
    private String suffix;

    /**
     * 获取配置信息,通过context获取我们配置文件中配置的信息
     * @param context
     */
    public void configure(Context context) {
        prefix = context.getString("prefix");
        //获取配置文件中配置的属性,没有则使用默认值
        suffix = context.getString("suffix","_aaa");
    }

    /**
     * 这个方法会反复的被调用
     * 在这里可以通过IO读取某个文件,也可以使用HDFS读取hdfs中的文件
     * 读取数据后,将其封装成一个Event
     * 再将event put到channel中
     * @return
     * @throws EventDeliveryException
     */
    public Status process() throws EventDeliveryException {

        Status status = null;

        try {
            Map<String, String> headers = new HashMap<String, String>();
            SimpleEvent event = new SimpleEvent();

            //1.获取数据
            for (int i = 0; i < 5; i++) {
                //设置头信息
                event.setHeaders(headers);
                //设置body数据
                event.setBody((prefix + "_" + i + suffix).getBytes());

                //将event写入到channel中
                getChannelProcessor().processEvent(event);
            }
            //设置为ready状态,表示准备好可以再次获取数据
            status = Status.READY;
        }catch (Exception e){
            e.printStackTrace();
            //失败则设置为backoff
            status = Status.BACKOFF;
        }

        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return status;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
