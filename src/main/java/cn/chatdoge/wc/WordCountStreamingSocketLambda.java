package cn.chatdoge.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class WordCountStreamingSocketLambda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env;
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS;
        socketDS = env.socketTextStream("localhost", 7777, "\n");

        // TODO 1. 拆分单词
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAddOne;

        wordAddOne = socketDS.flatMap(
                (String s, Collector<Tuple2<String,Integer>> collector)
                        ->
                {for (String word : s.split(" ")) {
                    collector.collect(Tuple2.of(word, 1));
                }
        }).returns(Types.TUPLE(Types.STRING,Types.INT)) // 必须显式返回flink流能识别的类型
        ;

        // TODO 2. 分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream;
        keyedStream = wordAddOne.keyBy(value -> value.f0);

        // TODO 3. 求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);

        sum.print();

        env.execute();
    }
}
