package dps.productionline.mqtt;

import dps.model.TelemetryMessage;
import dps.productionline.calibration.CalibrationCoordinator;
import dps.productionline.telemetry.SensorDataThread;

import java.util.ArrayList;

// Periodically publishes sensor averages together with the current operational state.
public class TelemetryPublisherThread extends Thread {
    private static final long PUBLICATION_INTERVAL_MS = 10000;

    private final int productionLineId;
    private final SensorDataThread sensorDataThread;
    private final CalibrationCoordinator coordinator;
    private final ProductionLineMqttClient mqttClient;
    private boolean running = true;

    public TelemetryPublisherThread(int productionLineId,
            SensorDataThread sensorDataThread,
            CalibrationCoordinator coordinator,
            ProductionLineMqttClient mqttClient) {
        this.productionLineId = productionLineId;
        this.sensorDataThread = sensorDataThread;
        this.coordinator = coordinator;
        this.mqttClient = mqttClient;
    }

    @Override
    public void run() {
        while (isRunning()) {
            try {
                Thread.sleep(PUBLICATION_INTERVAL_MS);
            } catch (InterruptedException e) {
                return;
            }

            if (!isRunning()) {
                return;
            }

            ArrayList<Double> averages = sensorDataThread.readAveragesAndClear();
            TelemetryMessage message = new TelemetryMessage(
                    productionLineId,
                    averages,
                    System.currentTimeMillis(),
                    coordinator.getState());

            // Keep failed batches available for the next publication attempt.
            if (!mqttClient.publishTelemetry(message)) {
                sensorDataThread.restoreAverages(averages);
            }
        }
    }

    public synchronized void shutdown() {
        running = false;
        interrupt();
    }

    private synchronized boolean isRunning() {
        return running;
    }
}
