import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) throws Exception {
        System.out.println("Loading WordCount ");

        ParameterTool params = ParameterTool.fromArgs(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataSet text;
        if (params.has("input")) {
            text = env.readTextFile(params.get("input"));
        } else {
            throw new Exception("No input file found");
        }

        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1).sortPartition(0, Order.ASCENDING);

        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }
        env.execute("Streaming WordCount");
    }

}

final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        Arrays.asList(value.toLowerCase().split("\\W+|[ ]|[0-9]+"))
                .forEach(token -> out.collect(new Tuple2(token, 1)));
    }
}
