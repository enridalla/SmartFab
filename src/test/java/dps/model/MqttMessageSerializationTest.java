package dps.model;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Verifies MQTT serialization preserves message fields, empty values, and nullable criticality.
class MqttMessageSerializationTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void telemetryMessageRoundTripPreservesAllFields() throws Exception {
        TelemetryMessage message = new TelemetryMessage(
                7,
                List.of(72.5, 81.25),
                123456,
                OperationalState.WaitingForCalibration);

        String json = objectMapper.writeValueAsString(message);
        TelemetryMessage decoded = objectMapper.readValue(json, TelemetryMessage.class);

        assertTrue(json.contains("\"productionLineId\":7"));
        assertTrue(json.contains("\"averages\":[72.5,81.25]"));
        assertTrue(json.contains("\"timestamp\":123456"));
        assertTrue(json.contains("\"currentState\":\"WaitingForCalibration\""));
        assertEquals(7, decoded.getProductionLineId());
        assertEquals(new ArrayList<>(List.of(72.5, 81.25)), decoded.getAverages());
        assertEquals(123456, decoded.getTimestamp());
        assertEquals(OperationalState.WaitingForCalibration, decoded.getCurrentState());
    }

    @Test
    void emptyTelemetryListAndNullableCriticalityRoundTrip() throws Exception {
        TelemetryMessage telemetry = new TelemetryMessage(
                3,
                List.of(),
                200,
                OperationalState.FullyOperational);
        StateUpdateMessage state = new StateUpdateMessage(
                3,
                OperationalState.FullyOperational,
                201,
                null);

        TelemetryMessage decodedTelemetry = objectMapper.readValue(
                objectMapper.writeValueAsString(telemetry),
                TelemetryMessage.class);
        StateUpdateMessage decodedState = objectMapper.readValue(
                objectMapper.writeValueAsString(state),
                StateUpdateMessage.class);

        assertTrue(decodedTelemetry.getAverages().isEmpty());
        assertEquals(3, decodedState.getProductionLineId());
        assertEquals(OperationalState.FullyOperational, decodedState.getNewState());
        assertEquals(201, decodedState.getTimestamp());
        assertNull(decodedState.getCriticality());
    }

    @Test
    void stateUpdateRoundTripPreservesCriticality() throws Exception {
        StateUpdateMessage message = new StateUpdateMessage(
                9,
                OperationalState.UnderCalibration,
                500,
                0.375);

        StateUpdateMessage decoded = objectMapper.readValue(
                objectMapper.writeValueAsString(message),
                StateUpdateMessage.class);

        assertEquals(9, decoded.getProductionLineId());
        assertEquals(OperationalState.UnderCalibration, decoded.getNewState());
        assertEquals(500, decoded.getTimestamp());
        assertEquals(0.375, decoded.getCriticality());
    }
}
