package cn.chatdoge.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class WordCountStreamingSocketLambda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env;
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS;
        socketDS = env.socketTextStream("localhost", 7777, "\n");

        // TODO 1. 拆分单词
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAddOne;
        wordAddOne = socketDS.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
            for (String word : s.split(" ")) {
                collector.collect(Tuple2.of(word, 1));
            }
        });

        // TODO 2. 分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream;
        keyedStream = wordAddOne.keyBy(
                (KeySelector<Tuple2<String, Integer>, String>) value -> value.f0);

        // TODO 3. 求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);

        sum.print();

        env.execute();
    }
}
