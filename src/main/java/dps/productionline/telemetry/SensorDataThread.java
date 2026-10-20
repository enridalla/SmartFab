package dps.productionline.telemetry;

import dps.model.OperationalState;
import dps.productionline.calibration.CalibrationCoordinator;
import dps.productionline.calibration.CalibrationThread;
import dps.productionline.mqtt.ProductionLineMqttClient;
import sensor.Measurement;
import sensor.MonitoringSensor;

import java.util.ArrayList;
import java.util.List;

// Aggregates sensor measurements and starts calibration when an average crosses the threshold.
public class SensorDataThread extends Thread {
    private final int productionLineId;
    private final MeasurementBuffer buffer;
    private final SlidingWindow slidingWindow;
    private final MonitoringSensor sensor;
    private final CalibrationCoordinator coordinator;
    private final ProductionLineMqttClient mqttClient;
    private volatile boolean running = true;
    // Averages are kept locally until a caller retrieves an atomic snapshot.
    private final ArrayList<Double> averages = new ArrayList<>();

    public SensorDataThread(int productionLineId, MeasurementBuffer buffer,
            SlidingWindow slidingWindow, MonitoringSensor sensor,
            CalibrationCoordinator coordinator,
            ProductionLineMqttClient mqttClient) {
        this.productionLineId = productionLineId;
        this.buffer = buffer;
        this.slidingWindow = slidingWindow;
        this.sensor = sensor;
        this.coordinator = coordinator;
        this.mqttClient = mqttClient;
    }

    @Override
    public void run() {
        while (running && !isInterrupted()) {
            try {
                // Wait for sensor data without polling.
                List<Measurement> measurements = buffer.waitAndReadAll();

                // Measurements produced around a calibration boundary are discarded.
                if (coordinator.getState() != OperationalState.FullyOperational) {
                    slidingWindow.clear();
                    continue;
                }

                processMeasurements(measurements);
            } catch (InterruptedException e) {
                interrupt();
            }
        }
    }

    public synchronized ArrayList<Double> readAveragesAndClear() {
        // Share the lock with storeAverage() to keep the snapshot atomic.
        ArrayList<Double> copy = new ArrayList<>(averages);
        averages.clear();
        return copy;
    }

    public synchronized void restoreAverages(ArrayList<Double> unpublishedAverages) {
        averages.addAll(0, unpublishedAverages);
    }

    public void shutdown() {
        // A library call can consume the interrupt; the shutdown decision must survive it.
        running = false;
        interrupt();
    }

    private void processMeasurements(List<Measurement> measurements) {
        for (Measurement measurement : measurements) {
            // A critical average may change the state in the middle of this batch.
            if (coordinator.getState() != OperationalState.FullyOperational) {
                slidingWindow.clear();
                return;
            }

            Double average = slidingWindow.addMeasurement(measurement);
            if (average == null) {
                continue;
            }

            storeAverage(average);
            System.out.printf("Line %d average vibration: %.2f%n", productionLineId, average);

            if (coordinator.detectCriticalCondition(average)) {
                // Stop sampling before the worker starts the distributed protocol.
                sensor.pauseMeasuring();
                buffer.clear();
                slidingWindow.clear();

                // Exit can reset coordinator criticality before this event is published.
                mqttClient.publishStateUpdate(
                        OperationalState.WaitingForCalibration,
                        coordinator.getStateTimestamp(),
                        CalibrationCoordinator.calculateCriticality(average));

                new CalibrationThread(
                        productionLineId,
                        coordinator,
                        sensor,
                        buffer,
                        slidingWindow,
                        mqttClient).start();
                return;
            }
        }
    }

    private synchronized void storeAverage(double average) {
        averages.add(average);
    }
}
