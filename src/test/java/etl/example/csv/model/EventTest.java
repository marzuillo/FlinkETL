package etl.example.csv.model;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class EventTest {

  @Test
  void to_map() {
    final Map<String, Object> map = new Event("GHA_1986", 1986, "Ghana", "GHA", "Inflation", 24.5654160765001, "1986-12-31T23:59:59.000").toMap();
    Assertions.assertThat(map.get("Inflation")).isEqualTo(24.5654160765001);
  }
}