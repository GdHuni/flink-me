package datasource.custom_datasource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @Classname SourceFromKafka
 * @Description  有并行度的自定义数据源，数据会在所有的分区都输出一次
 * @Date 2021/11/8 17:53
 * @Created by huni
 */

public class SelfSourceParallel implements ParallelSourceFunction<String> {
    long count = 0;
    boolean isRunning = true;
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(isRunning) {
            ctx.collect(String.valueOf(count));
            count ++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
