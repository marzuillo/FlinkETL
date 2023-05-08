package etl.example.csv;

import etl.example.csv.aggregator.EventsAggregateFunction;
import etl.example.csv.deserializer.LogEventProcessor;
import etl.example.csv.model.Event;
import etl.example.csv.model.EventCollection;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Application {
    private static final Logger LOG = LoggerFactory.getLogger(Application.class);
    public static final String LOG_STRING_FILE = "/Users/" + System.getenv("USER") + "/repo/flink/src/main/resources/log.txt";
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    public static void main(String[] args) {
        try {
            etl();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void etl() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        LOG.info("going to read {} ", LOG_STRING_FILE);
        final DataStreamSource<String> stringDataStreamSource = env.readTextFile(LOG_STRING_FILE);

        MapFunction<String, Event> mapFunction = (eventLogRow -> LogEventProcessor.toEvent(eventLogRow)) ;
        stringDataStreamSource
                .map(mapFunction)
                .filter(Objects::nonNull)
                .keyBy(event -> event.getId())
                .window(EventTimeSessionWindows.withGap(Time.minutes(2)))
                .aggregate(new EventsAggregateFunction())
                .sinkTo(elasticSearchSink());

        LOG.info("event sink completed");
        env.execute("text example complete");
    }

    private static ElasticsearchSink<Map<String, Object>> elasticSearchSink() {
        return new Elasticsearch7SinkBuilder<Map<String, Object>>()
                .setBulkFlushMaxActions(1) // Instructs the sink to emit after every element, otherwise they would be buffered
                .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
                .setEmitter(
                        (element, context, indexer) ->
                                indexer.add(createIndexRequest(element))
                ).build();
    }

    private static IndexRequest createIndexRequest(Map<String, Object> element) {
        return Requests.indexRequest()
                .index("my-index-"+ dateTimeFormatter.format(LocalDateTime.now()))
                .source(element);
    }


}
