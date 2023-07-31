package cn.chatdoge.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Arrays;


/**
 * 从集合中读取数据
 * 一般用于测试
 *
 * @author simon.mau
 * @date 2023/7/29
 */
public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Arrays.asList的返回值才是Collection
        DataStreamSource<Integer> collectionDS = env.fromCollection(Arrays.asList(1, 2, 3, 11, 22, 33));
        collectionDS.print();

        env.execute();
    }
}
