package com.chatdoge.wc;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class word_count_streaming_demo {
    public static void main(String[] args) {
        // TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 2.读取数据
        DataStreamSource<String> lineDS = env.readTextFile("input/words.txt");

        // TODO 3.切分 转换 分组 聚合
        lineDS.flatMap();

        // TODO 4.打印输出

        // TODO 5.执行
    }
}
