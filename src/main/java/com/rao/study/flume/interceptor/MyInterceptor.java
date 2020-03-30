package com.rao.study.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class MyInterceptor implements Interceptor {
    public void initialize() {

    }

    public Event intercept(Event event) {
        byte[] body = event.getBody();
        if (body[0] < 'z' && body[0] > 'a') {
            event.getHeaders().put("type", "letter");
        } else if (body[0] > '0' && body[0] < '9') {
            event.getHeaders().put("type", "number");
        }
        return event;
    }

    public List<Event> intercept(List<Event> list) {
        for (Event event:list){
            intercept(event);
        }
        return list;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        /**
         * 通过静态内部类的方式创建实例
         * @return
         */
        public Interceptor build() {
            return new MyInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
