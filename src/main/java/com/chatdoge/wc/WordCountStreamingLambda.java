package com.chatdoge.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamingLambda {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("input/words.txt")
                .flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                    for (String word : s.split(" ")) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING,Types.INT))  //java类型擦除问题,必须显示指定返回类型
                .keyBy(value -> value.f0)
                .sum(1)
                .print();
        env.execute();
    }
}
