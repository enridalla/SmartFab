package dps.productionline.telemetry;

import sensor.Measurement;

import java.util.ArrayList;

// Computes averages over eight measurements with four samples shared between successive windows.
public class SlidingWindow {
    private static final int WINDOW_SIZE = 8;
    private static final int WINDOW_OVERLAP = 4;
    private static final int STEP = WINDOW_SIZE - WINDOW_OVERLAP;

    // Retain the WINDOW_OVERLAP most recent samples after each average so they
    // become the head of the next window.
    private final ArrayList<Measurement> measurements = new ArrayList<>();

    public synchronized Double addMeasurement(Measurement measurement) {
        measurements.add(measurement);

        if (measurements.size() < WINDOW_SIZE) {
            return null;
        }

        double sum = 0;
        for (Measurement current : measurements) {
            sum += current.value();
        }

        double average = sum / WINDOW_SIZE;

        // Keep the overlapping samples for the next window.
        for (int i = 0; i < STEP; i++) {
            measurements.remove(0);
        }

        return average;
    }

    public synchronized void clear() {
        // Do not combine a partial window with measurements from a new operating period.
        measurements.clear();
    }

    synchronized int size() {
        return measurements.size();
    }
}
