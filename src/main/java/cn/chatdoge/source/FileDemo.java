package cn.chatdoge.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从文件中读取数据
 * 通用写法fromSource要求传参一个Source对象,这是个接口,有自己的public实现类
 *
 * @author simon.mau
 * @date 2023/7/29
 */
public class FileDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> source = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("input/words.txt"))
                .build();
        DataStreamSource<String> fileDS = env
                .fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "fileDS");
        fileDS.print();

        env.execute();
    }
}
