package dps.productionline.telemetry;

import org.junit.jupiter.api.Test;
import sensor.Measurement;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Verifies sliding window averages, overlapping measurements, and window clearing.
class SlidingWindowTest {
    @Test
    void eightMeasurementsProduceOneAverageAndKeepTheOverlap() {
        SlidingWindow window = new SlidingWindow();

        ArrayList<Double> averages = addMeasurements(window, 1, 8);

        assertEquals(1, averages.size());
        assertEquals(4.5, averages.get(0));
        assertEquals(4, window.size());
    }

    @Test
    void twelveMeasurementsProduceWindowsOneToEightAndFiveToTwelve() {
        SlidingWindow window = new SlidingWindow();

        ArrayList<Double> averages = addMeasurements(window, 1, 12);

        assertEquals(2, averages.size());
        assertEquals(4.5, averages.get(0));
        assertEquals(8.5, averages.get(1));
        assertEquals(4, window.size());
    }

    @Test
    void clearRemovesTheOverlappingMeasurements() {
        SlidingWindow window = new SlidingWindow();
        addMeasurements(window, 1, 8);

        window.clear();

        assertEquals(0, window.size());
    }

    // Feeds the values one at a time, exactly like SensorDataThread does.
    private ArrayList<Double> addMeasurements(SlidingWindow window, int first, int last) {
        ArrayList<Double> averages = new ArrayList<>();

        for (int value = first; value <= last; value++) {
            Double average = window.addMeasurement(
                    new Measurement("sensor", "Vibration", value, value));

            if (average != null) {
                averages.add(average);
            }
        }

        return averages;
    }
}
