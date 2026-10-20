package dps.productionline.telemetry;

import sensor.Buffer;
import sensor.Measurement;

import java.util.ArrayList;
import java.util.List;

// Buffers sensor readings and lets consumers wait until measurements are available.
public class MeasurementBuffer implements Buffer {
    private final ArrayList<Measurement> measurements = new ArrayList<>();

    @Override
    public synchronized void addMeasurement(Measurement measurement) {
        measurements.add(measurement);

        // Wake the consumer if it is waiting for the first measurement in a batch.
        notifyAll();
    }

    @Override
    public synchronized List<Measurement> readAllAndClear() {
        ArrayList<Measurement> copy = new ArrayList<>(measurements);
        measurements.clear();
        return copy;
    }

    @Override
    public synchronized void clear() {
        // Calibration boundaries discard readings from the previous operating period.
        measurements.clear();
    }

    public synchronized List<Measurement> waitAndReadAll() throws InterruptedException {
        while (measurements.isEmpty()) {
            wait();
        }

        return readAllAndClear();
    }
}
