package cn.chatdoge.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamingTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> LineDS = env.readTextFile("input/words.txt");
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAddOne = LineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    collector.collect(wordTuple2);
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> wordAddOneGroupBy = wordAddOne.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAddOneSum = wordAddOneGroupBy.sum(1);
        wordAddOneSum.print();
        env.execute();
    }
}
