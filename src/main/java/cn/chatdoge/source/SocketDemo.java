package cn.chatdoge.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从模拟的通讯端点接收数据,比较简单
 *
 * @author simon.mau
 * @date 2023/7/29
 */
public class SocketDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);
        socketDS.print();

        env.execute();
    }

}
