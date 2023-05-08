package etl.example.csv.deserializer;

import etl.example.csv.model.Event;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class LogEventProcessorTest {

  @Test
  void parse_log() {
    final Event event = LogEventProcessor.toEvent("id=GHA_1986, year=1986, country=Ghana, countryCode=GHA, indicator=Inflation, value=24.5654160765001, timestamp=1986-12-31'T'23:59:59.000");
    Assertions.assertThat(event).isEqualTo(
        new Event("GHA_1986", 1986, "Ghana", "GHA", "Inflation", 24.5654160765001, "1986-12-31'T'23:59:59.000")
    );
  }
}