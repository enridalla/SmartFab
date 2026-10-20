package dps.productionline.telemetry;

import dps.model.OperationalState;
import dps.productionline.PeerList;
import dps.productionline.calibration.CalibrationCoordinator;
import dps.productionline.mqtt.ProductionLineMqttClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import sensor.Measurement;
import sensor.MonitoringSensor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Verifies failed batch preservation and telemetry races during controlled exit.
class SensorDataThreadTest {
    @Test
    void unpublishedAveragesAreRestoredBeforeNewerValues() {
        SensorDataThread thread = new SensorDataThread(
                1, null, null, null, null, null);

        thread.restoreAverages(new ArrayList<>(List.of(3.0)));
        thread.restoreAverages(new ArrayList<>(List.of(1.0, 2.0)));

        assertEquals(List.of(1.0, 2.0, 3.0), thread.readAveragesAndClear());
        assertEquals(List.of(), thread.readAveragesAndClear());
    }

    @Test
    @Timeout(10)
    void shutdownStillStopsTheConsumerWhenMqttConsumesTheInterrupt() throws Exception {
        MeasurementBuffer buffer = new MeasurementBuffer();
        MonitoringSensor sensor = new MonitoringSensor(buffer);
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, new PeerList(1));
        CountDownLatch publicationStarted = new CountDownLatch(1);
        CountDownLatch interruptConsumed = new CountDownLatch(1);
        ProductionLineMqttClient mqtt = new ProductionLineMqttClient(1) {
            @Override
            public boolean publishStateUpdate(OperationalState state, long timestamp,
                    Double criticality) {
                publicationStarted.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    // Match Paho's failed token wait, which does not restore the interrupt flag.
                    interruptConsumed.countDown();
                }
                return false;
            }
        };
        SensorDataThread thread = new SensorDataThread(
                1, buffer, new SlidingWindow(), sensor, coordinator, mqtt);

        // Supply a complete critical window without starting the random simulator.
        for (int i = 0; i < 8; i++) {
            buffer.addMeasurement(new Measurement("test", "Vibration", 96, i));
        }
        thread.start();
        try {
            assertTrue(publicationStarted.await(2, TimeUnit.SECONDS));
            assertEquals(OperationalState.WaitingForCalibration, coordinator.getState());

            coordinator.requestExit();
            sensor.stopMeasuring();
            thread.shutdown();

            assertTrue(interruptConsumed.await(2, TimeUnit.SECONDS));
            thread.join(2000);
            assertFalse(thread.isAlive(), "Sensor consumer stayed blocked after controlled exit");
            assertTimeoutPreemptively(Duration.ofSeconds(2), coordinator::waitUntilReadyToExit);
        } finally {
            // A second interruption also cleans up the original buggy implementation on failure.
            coordinator.requestExit();
            sensor.stopMeasuring();
            thread.shutdown();
            thread.join(2000);
        }
    }

    @Test
    @Timeout(10)
    void waitingUpdateKeepsTheTriggerCriticalityWhenExitCancelsTheRound() throws Exception {
        MeasurementBuffer buffer = new MeasurementBuffer();
        MonitoringSensor sensor = new MonitoringSensor(buffer);
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, new PeerList(1)) {
            @Override
            public synchronized boolean detectCriticalCondition(double average) {
                boolean detected = super.detectCriticalCondition(average);
                if (detected) {
                    // Force exit between detecting the critical window and publishing its event.
                    requestExit();
                }
                return detected;
            }
        };
        CountDownLatch published = new CountDownLatch(1);
        Double[] publishedCriticality = new Double[1];
        ProductionLineMqttClient mqtt = new ProductionLineMqttClient(1) {
            @Override
            public boolean publishStateUpdate(OperationalState state, long timestamp,
                    Double criticality) {
                publishedCriticality[0] = criticality;
                published.countDown();
                return true;
            }
        };
        SensorDataThread thread = new SensorDataThread(
                1, buffer, new SlidingWindow(), sensor, coordinator, mqtt);
        for (int i = 0; i < 8; i++) {
            buffer.addMeasurement(new Measurement("test", "Vibration", 96, i));
        }

        thread.start();
        try {
            assertTrue(published.await(2, TimeUnit.SECONDS));
            assertEquals(0.0, coordinator.getCriticality());
            assertEquals(0.2, publishedCriticality[0], 0.000001);
        } finally {
            coordinator.requestExit();
            sensor.stopMeasuring();
            thread.shutdown();
            thread.join(2000);
            assertFalse(thread.isAlive());
            assertTimeoutPreemptively(Duration.ofSeconds(2), coordinator::waitUntilReadyToExit);
        }
    }
}
