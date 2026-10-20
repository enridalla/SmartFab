package dps.productionline.telemetry;

import org.junit.jupiter.api.Test;
import sensor.Measurement;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Verifies measurement ordering and buffer clearing after reading or explicit removal.
class MeasurementBufferTest {
    @Test
    void addAndReadAllAndClear() {
        MeasurementBuffer buffer = new MeasurementBuffer();
        Measurement first = measurement(10, 1);
        Measurement second = measurement(20, 2);

        buffer.addMeasurement(first);
        buffer.addMeasurement(second);

        List<Measurement> measurements = buffer.readAllAndClear();

        assertEquals(List.of(first, second), measurements);
        assertTrue(buffer.readAllAndClear().isEmpty());
    }

    @Test
    void clearRemovesAllMeasurements() {
        MeasurementBuffer buffer = new MeasurementBuffer();
        buffer.addMeasurement(measurement(10, 1));

        buffer.clear();

        assertTrue(buffer.readAllAndClear().isEmpty());
    }

    private Measurement measurement(double value, long timestamp) {
        return new Measurement("sensor", "Vibration", value, timestamp);
    }
}
