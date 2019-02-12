import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Splitter;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class KMeans {

    public static void main(String[] args) throws Exception {
        System.out.println("Loading Kmeans ");

        ParameterTool params = ParameterTool.fromArgs(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        boolean[] boolArray = {true, false, true, false, true, false, true, true};
        DataSet<Tuple5<String, Integer, Integer, Double, Double>> ds;

        if (!params.has("input")) {
            ds = //env.readCsvFile(params.get("input"))
                env.readCsvFile(KMeans.class.getResource("germany.csv").getPath())
                    .ignoreFirstLine()
                    .includeFields(boolArray)
                    .types(String.class, Integer.class, Integer.class, Double.class, Double.class);
            System.out.println("Data loaded : " + ds.count());
        } else {
            throw new Exception("No input file found");
        }

        // MNC filter : Assignment point 2
        if (params.has("mnc")) {
            ds = ds.filter(mncFilter(params.get("mnc")));
        }

        // Default cluster value  : Assignment point 3
        String[] radioTypeArray = {"LTE"};
        Long lteTowerCount = ds.filter(lteTowerFilter(Arrays.asList(radioTypeArray))).flatMap(new TowerCounterMap()).count();
        Long kParam = params.getLong("k", lteTowerCount);

        // MAX value  : Assignment point 4
        Long maxKParam = Math.max(kParam, lteTowerCount);

        DataSet<Centroid> centroids = getCentroidDataSet(ds, 10);
        DataSet<Point> points = getPointDataSet(ds);

        System.out.println("Total centroids : " + centroids.count() + " and Total points : "+points.count());

        IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

        DataSet<Centroid> newCentroids = points
                // compute closest centroid for each point
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                // count and sum point coordinates for each centroid
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                // compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());


        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                // assign points to final clusters
                .map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

        // emit result
        if (params.has("output")) {
            clusteredPoints.writeAsCsv(params.get("output"), "\n", " ");

            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("KMeans Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            clusteredPoints.print();
        }

        //env.execute("Streaming KMeans");
    }


    private static DataSet<Centroid> getCentroidDataSet(DataSet<Tuple5<String, Integer, Integer, Double, Double>> ds, int kParam) {
        String[] radioTypeArray = {"LTE"};
        return ds.filter(lteTowerFilter(Arrays.asList(radioTypeArray))).flatMap(new CentroidMap()).first(kParam);
    }

    private static DataSet<Point> getPointDataSet(DataSet<Tuple5<String, Integer, Integer, Double, Double>> ds) {
        String[] radioTypeArray = {"GSM", "UMTS"};
        return ds.filter(lteTowerFilter(Arrays.asList(radioTypeArray))).flatMap(new PointMap());
    }


    private static FilterFunction<Tuple5<String, Integer, Integer, Double, Double>> mncFilter(String mncParam) {
        return new RichFilterFunction<Tuple5<String, Integer, Integer, Double, Double>>() {
            @Override
            public boolean filter(Tuple5<String, Integer, Integer, Double, Double> csvData) {
                return Arrays.asList(Splitter.on(",").split(mncParam)).stream().anyMatch(csvData.f2::equals);
            }
        };
    }

    private static FilterFunction<Tuple5<String, Integer, Integer, Double, Double>> lteTowerFilter(List<String> radioType) {
        return new RichFilterFunction<Tuple5<String, Integer, Integer, Double, Double>>() {
            @Override
            public boolean filter(Tuple5<String, Integer, Integer, Double, Double> csvData) {
                return radioType.stream().anyMatch(csvData.f0::equalsIgnoreCase);
            }
        };
    }
}


final class TowerCounterMap extends RichFlatMapFunction<Tuple5<String, Integer, Integer, Double, Double>, Tuple1<Integer>> {

    @Override
    public void flatMap(
            Tuple5<String, Integer, Integer, Double, Double> value,
            Collector<Tuple1<Integer>> out) {
        out.collect(new Tuple1<Integer>(1));
    }
}

final class CentroidMap extends RichFlatMapFunction<Tuple5<String, Integer, Integer, Double, Double>, Centroid> {

    @Override
    public void flatMap(
            Tuple5<String, Integer, Integer, Double, Double> value,
            Collector<Centroid> out) {
        out.collect(new Centroid(value.f2, value.f3, value.f4));
    }
}


final class PointMap extends RichFlatMapFunction<Tuple5<String, Integer, Integer, Double, Double>, Point> {

    @Override
    public void flatMap(
            Tuple5<String, Integer, Integer, Double, Double> value,
            Collector<Point> out) {
        out.collect(new Point(value.f3, value.f4));
    }
}

class Point implements Serializable {

    public double x, y;

    public Point() {
    }

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public Point add(Point other) {
        x += other.x;
        y += other.y;
        return this;
    }

    public Point div(long val) {
        x /= val;
        y /= val;
        return this;
    }

    public double euclideanDistance(Point other) {
        return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
    }

    public void clear() {
        x = y = 0.0;
    }

    @Override
    public String toString() {
        return x + " " + y;
    }
}

class Centroid extends Point {

    public int id;

    public Centroid() {
    }

    public Centroid(int id, double x, double y) {
        super(x, y);
        this.id = id;
    }

    public Centroid(int id, Point p) {
        super(p.x, p.y);
        this.id = id;
    }

    @Override
    public String toString() {
        return id + " " + super.toString();
    }
}


final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
    private Collection<Centroid> centroids;

    /**
     * Reads the centroid values from a broadcast variable into a collection.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
    }

    @Override
    public Tuple2<Integer, Point> map(Point p) throws Exception {

        double minDistance = Double.MAX_VALUE;
        int closestCentroidId = -1;

        // check all cluster centers
        for (Centroid centroid : centroids) {
            // compute distance
            double distance = p.euclideanDistance(centroid);

            // update nearest cluster if necessary
            if (distance < minDistance) {
                minDistance = distance;
                closestCentroidId = centroid.id;
            }
        }

        // emit a new record with the center id and the data point.
        return new Tuple2<>(closestCentroidId, p);
    }
}

final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

    @Override
    public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
        return new Tuple3<>(t.f0, t.f1, 1L);
    }
}

/**
 * Sums and counts point coordinates.
 */
final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

    @Override
    public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
        return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
    }
}

/**
 * Computes new centroid from coordinate sum and count of points.
 */
final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

    @Override
    public Centroid map(Tuple3<Integer, Point, Long> value) {
        return new Centroid(value.f0, value.f1.div(value.f2));
    }
}