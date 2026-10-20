package dps.administration;

import dps.model.OperationalState;
import dps.model.ProductionLineInfo;
import dps.model.ProductionLineState;
import dps.model.StateUpdateMessage;
import dps.model.TelemetryMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Verifies registration, telemetry queries, and removal through the administration endpoints.
class ProductionLineControllerTest {
    private ProductionLineRegistry registry;
    private TelemetryStore telemetryStore;
    private OperationalStateStore stateStore;
    private ProductionLineController controller;

    @BeforeEach
    void setUp() {
        registry = new ProductionLineRegistry();
        telemetryStore = new TelemetryStore();
        stateStore = new OperationalStateStore();
        controller = new ProductionLineController(registry, telemetryStore, stateStore);
    }

    @Test
    void registrationInitializesStateAndGetEndpointsReturnCurrentLines() {
        ResponseEntity<List<ProductionLineInfo>> firstResponse = controller.register(line(1, 50051));
        ResponseEntity<List<ProductionLineInfo>> secondResponse = controller.register(line(2, 50052));

        assertEquals(HttpStatus.CREATED, firstResponse.getStatusCode());
        assertTrue(firstResponse.getBody().isEmpty());
        assertEquals(HttpStatus.CREATED, secondResponse.getStatusCode());
        assertEquals(1, secondResponse.getBody().size());
        assertEquals(1, secondResponse.getBody().get(0).getId().intValue());

        List<ProductionLineInfo> lines = controller.getProductionLines();
        assertEquals(2, lines.size());
        assertEquals(1, lines.get(0).getId().intValue());
        assertEquals(2, lines.get(1).getId().intValue());

        List<ProductionLineState> states = controller.getProductionLineStates();
        assertEquals(2, states.size());
        assertEquals(1, states.get(0).getProductionLineId().intValue());
        assertEquals(OperationalState.FullyOperational, states.get(0).getOperationalState());
        assertEquals(2, states.get(1).getProductionLineId().intValue());
        assertEquals(OperationalState.FullyOperational, states.get(1).getOperationalState());
    }

    @Test
    void averageUsesInclusiveRangeAndReturnsExpectedStatuses() {
        controller.register(line(1, 50051));
        long base = System.currentTimeMillis() + 100;
        telemetryStore.addTelemetry(telemetry(1, base - 1, 1000));
        telemetryStore.addTelemetry(telemetry(1, base, 10, 20));
        telemetryStore.addTelemetry(telemetry(1, base + 100, 30));
        telemetryStore.addTelemetry(telemetry(1, base + 101, 1000));

        ResponseEntity<Double> average = controller.getAverage(1, base, base + 100);
        assertEquals(HttpStatus.OK, average.getStatusCode());
        assertEquals(20.0, average.getBody());

        assertEquals(HttpStatus.NO_CONTENT,
                controller.getAverage(1, base + 300, base + 400).getStatusCode());
        assertEquals(HttpStatus.BAD_REQUEST,
                controller.getAverage(1, base + 200, base + 100).getStatusCode());
        assertEquals(HttpStatus.NOT_FOUND, controller.getAverage(99, 0, 1000).getStatusCode());
    }

    @Test
    void registrationAndEventsUseTheRemoteLinesClock() {
        ProductionLineInfo remoteLine = line(1, 50051);
        long remoteStart = System.currentTimeMillis() - 3600000;
        remoteLine.setRegistrationTimestamp(remoteStart);
        assertEquals(HttpStatus.CREATED, controller.register(remoteLine).getStatusCode());

        assertFalse(telemetryStore.addTelemetry(telemetry(1, remoteStart - 1, 999)));
        assertTrue(telemetryStore.addTelemetry(telemetry(1, remoteStart + 10000, 90)));
        assertTrue(stateStore.updateState(new StateUpdateMessage(
                1, OperationalState.WaitingForCalibration, remoteStart + 10000, 0.125)));
        assertEquals(90.0, controller.getAverage(1, remoteStart, remoteStart + 10000).getBody());
        assertEquals(OperationalState.WaitingForCalibration, stateStore.getState(1));
        assertEquals(remoteStart, registry.getProductionLines().get(0).getRegistrationTimestamp());
    }

    @Test
    void deletePurgesLineDataAndRejoinStartsClean() {
        controller.register(line(1, 50051));
        long timestamp = System.currentTimeMillis() + 100;
        telemetryStore.addTelemetry(telemetry(1, timestamp, 42));
        stateStore.updateState(new StateUpdateMessage(
                1, OperationalState.UnderCalibration, timestamp, 0.9));

        assertEquals(HttpStatus.NO_CONTENT, controller.remove(1).getStatusCode());
        assertFalse(registry.contains(1));
        assertNull(telemetryStore.calculateAverage(1, 0, 1000));
        assertNull(stateStore.getState(1));
        assertEquals(HttpStatus.NOT_FOUND, controller.remove(1).getStatusCode());

        assertEquals(HttpStatus.CREATED, controller.register(line(1, 50061)).getStatusCode());
        assertTrue(registry.contains(1));
        assertEquals(OperationalState.FullyOperational, stateStore.getState(1));
        assertEquals(HttpStatus.NO_CONTENT, controller.getAverage(1, 0, 1000).getStatusCode());
    }

    private ProductionLineInfo line(int id, int port) {
        return new ProductionLineInfo(id, "127.0.0.1", port);
    }

    private TelemetryMessage telemetry(int id, long timestamp, double... values) {
        Double[] averages = new Double[values.length];
        for (int i = 0; i < values.length; i++) {
            averages[i] = values[i];
        }

        return new TelemetryMessage(
                id,
                List.of(averages),
                timestamp,
                OperationalState.FullyOperational);
    }
}
