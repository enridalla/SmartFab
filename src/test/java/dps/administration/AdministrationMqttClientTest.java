package dps.administration;

import dps.model.OperationalState;
import dps.mqtt.MqttConfiguration;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

// Verifies MQTT message routing and rejection of malformed or unknown production line messages.
class AdministrationMqttClientTest {
    @Test
    void validTelemetryIsStoredWithoutChangingOperationalState() {
        TelemetryStore telemetryStore = new TelemetryStore();
        OperationalStateStore stateStore = new OperationalStateStore();
        telemetryStore.addProductionLine(1);
        stateStore.addProductionLine(1);
        stateStore.addProductionLine(2);
        AdministrationMqttClient client = new AdministrationMqttClient(
                telemetryStore,
                stateStore);

        client.messageArrived(
                MqttConfiguration.TELEMETRY_TOPIC,
                message("{\"productionLineId\":1,\"averages\":[10.0,20.0],"
                        + "\"timestamp\":100,\"currentState\":\"WaitingForCalibration\"}"));

        assertEquals(1, telemetryStore.getBatchCount(1));
        assertEquals(15.0, telemetryStore.calculateAverage(1, 100, 100));
        assertEquals(OperationalState.FullyOperational, stateStore.getState(1));
        assertEquals(OperationalState.FullyOperational, stateStore.getState(2));
    }

    @Test
    void validStateUpdateChangesStateWithoutStoringTelemetry() {
        TelemetryStore telemetryStore = new TelemetryStore();
        OperationalStateStore stateStore = new OperationalStateStore();
        telemetryStore.addProductionLine(1);
        stateStore.addProductionLine(1);
        stateStore.addProductionLine(2);
        AdministrationMqttClient client = new AdministrationMqttClient(
                telemetryStore,
                stateStore);

        client.messageArrived(
                MqttConfiguration.STATE_TOPIC,
                message("{\"productionLineId\":1,\"newState\":\"WaitingForCalibration\","
                        + "\"timestamp\":101,\"criticality\":0.75}"));

        assertEquals(OperationalState.WaitingForCalibration, stateStore.getState(1));
        assertEquals(OperationalState.FullyOperational, stateStore.getState(2));
        assertEquals(0, telemetryStore.getBatchCount(1));
    }

    @Test
    void malformedTelemetryAndStatePayloadsAreIgnored() {
        TelemetryStore telemetryStore = new TelemetryStore();
        OperationalStateStore stateStore = new OperationalStateStore();
        telemetryStore.addProductionLine(1);
        stateStore.addProductionLine(1);
        AdministrationMqttClient client = new AdministrationMqttClient(
                telemetryStore,
                stateStore);

        assertDoesNotThrow(() -> client.messageArrived(
                MqttConfiguration.TELEMETRY_TOPIC,
                message("{malformed telemetry")));
        assertDoesNotThrow(() -> client.messageArrived(
                MqttConfiguration.STATE_TOPIC,
                message("{malformed state")));

        assertEquals(0, telemetryStore.getBatchCount(1));
        assertEquals(OperationalState.FullyOperational, stateStore.getState(1));
    }

    @Test
    void messagesFromUnknownProductionLinesAreIgnored() {
        TelemetryStore telemetryStore = new TelemetryStore();
        OperationalStateStore stateStore = new OperationalStateStore();
        telemetryStore.addProductionLine(1);
        stateStore.addProductionLine(1);
        AdministrationMqttClient client = new AdministrationMqttClient(
                telemetryStore,
                stateStore);

        client.messageArrived(
                MqttConfiguration.TELEMETRY_TOPIC,
                message("{\"productionLineId\":99,\"averages\":[30.0],"
                        + "\"timestamp\":200,\"currentState\":\"FullyOperational\"}"));
        client.messageArrived(
                MqttConfiguration.STATE_TOPIC,
                message("{\"productionLineId\":99,\"newState\":\"UnderCalibration\","
                        + "\"timestamp\":201,\"criticality\":0.5}"));

        assertEquals(0, telemetryStore.getBatchCount(1));
        assertEquals(0, telemetryStore.getBatchCount(99));
        assertEquals(OperationalState.FullyOperational, stateStore.getState(1));
        assertNull(stateStore.getState(99));
    }

    @Test
    void unknownTopicDoesNotChangeEitherStore() {
        TelemetryStore telemetryStore = new TelemetryStore();
        OperationalStateStore stateStore = new OperationalStateStore();
        telemetryStore.addProductionLine(1);
        stateStore.addProductionLine(1);
        AdministrationMqttClient client = new AdministrationMqttClient(
                telemetryStore,
                stateStore);

        client.messageArrived(
                "smartfab/unknown",
                message("{\"productionLineId\":1,\"averages\":[40.0],"
                        + "\"timestamp\":300,\"currentState\":\"UnderCalibration\"}"));

        assertEquals(0, telemetryStore.getBatchCount(1));
        assertEquals(OperationalState.FullyOperational, stateStore.getState(1));
    }

    private MqttMessage message(String payload) {
        return new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
    }
}
