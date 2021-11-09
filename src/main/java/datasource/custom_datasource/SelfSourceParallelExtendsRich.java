package datasource.custom_datasource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class SelfSourceParallelExtendsRich extends RichParallelSourceFunction<String> {
    long count;
    boolean isRunning;

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("....open");
        count = 0;
        isRunning = true;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(isRunning) {
            ctx.collect(String.valueOf(count) + "...from Rich");
            count ++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
