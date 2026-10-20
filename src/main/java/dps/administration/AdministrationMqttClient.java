package dps.administration;

import dps.model.OperationalState;
import dps.model.StateUpdateMessage;
import dps.model.TelemetryMessage;
import dps.mqtt.MqttConfiguration;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

// Receives MQTT telemetry and state updates for the administration stores.
@Component
public class AdministrationMqttClient implements MqttCallback {
    private final TelemetryStore telemetryStore;
    private final OperationalStateStore stateStore;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private MqttClient client;

    @Autowired
    public AdministrationMqttClient(TelemetryStore telemetryStore,
            OperationalStateStore stateStore) {
        this.telemetryStore = telemetryStore;
        this.stateStore = stateStore;
    }

    @PostConstruct
    public void start() {
        if (!connect()) {
            System.out.println("Administration Server is running, but MQTT monitoring is unavailable");
        }
    }

    public synchronized boolean connect() {
        if (client != null && client.isConnected()) {
            return true;
        }

        closeClient();
        try {
            client = new MqttClient(
                    MqttConfiguration.BROKER_ADDRESS,
                    MqttClient.generateClientId(),
                    new MemoryPersistence());
            client.setTimeToWait(5000);

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setConnectionTimeout(5);

            client.setCallback(this);
            client.connect(options);
            client.subscribe(
                    new String[]{
                            MqttConfiguration.TELEMETRY_TOPIC,
                            MqttConfiguration.STATE_TOPIC
                    },
                    new int[]{MqttConfiguration.QOS, MqttConfiguration.QOS});

            System.out.println("Administration Server subscribed to MQTT topics");
            return true;
        } catch (MqttException | IllegalArgumentException e) {
            System.out.println("Cannot connect Administration Server to MQTT broker "
                    + MqttConfiguration.BROKER_ADDRESS + ": " + e.getMessage());
            disconnect();
            return false;
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) {
        String payload = new String(mqttMessage.getPayload(), StandardCharsets.UTF_8);

        try {
            if (MqttConfiguration.TELEMETRY_TOPIC.equals(topic)) {
                receiveTelemetry(objectMapper.readValue(payload, TelemetryMessage.class));
            } else if (MqttConfiguration.STATE_TOPIC.equals(topic)) {
                receiveStateUpdate(objectMapper.readValue(payload, StateUpdateMessage.class));
            }
        } catch (Exception e) {
            // Keep malformed payloads from interrupting the MQTT callback.
            System.out.println("Ignoring malformed MQTT message on " + topic + ": " + e.getMessage());
        }
    }

    private void receiveTelemetry(TelemetryMessage message) {
        if (!isValidTelemetry(message)) {
            System.out.println("Ignoring invalid telemetry message");
            return;
        }

        if (!telemetryStore.addTelemetry(message)) {
            System.out.println("Ignoring telemetry from inactive or superseded line "
                    + message.getProductionLineId());
        }
    }

    private void receiveStateUpdate(StateUpdateMessage message) {
        if (!isValidStateUpdate(message)) {
            System.out.println("Ignoring invalid state update");
            return;
        }

        if (!stateStore.updateState(message)) {
            System.out.println("Ignoring state update from inactive line " + message.getProductionLineId());
        }
    }

    private boolean isValidTelemetry(TelemetryMessage message) {
        if (message == null
                || message.getProductionLineId() == null
                || message.getAverages() == null
                || message.getTimestamp() < 0
                || message.getCurrentState() == null) {
            return false;
        }

        for (Double average : message.getAverages()) {
            if (average == null || !Double.isFinite(average)) {
                return false;
            }
        }

        return true;
    }

    private boolean isValidStateUpdate(StateUpdateMessage message) {
        if (message == null
                || message.getProductionLineId() == null
                || message.getNewState() == null
                || message.getTimestamp() < 0) {
            return false;
        }

        // Calibration states require the criticality that triggered the request.
        OperationalState state = message.getNewState();
        if (state == OperationalState.WaitingForCalibration
                || state == OperationalState.UnderCalibration) {
            return message.getCriticality() != null
                    && Double.isFinite(message.getCriticality())
                    && message.getCriticality() >= 0;
        }

        return message.getCriticality() == null
                || Double.isFinite(message.getCriticality());
    }

    @Override
    public void connectionLost(Throwable cause) {
        String message = cause == null ? "unknown cause" : cause.getMessage();
        System.out.println("Administration Server lost the MQTT connection: " + message);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // This client only subscribes, so delivery acknowledgements need no handling.
    }

    @PreDestroy
    public synchronized void disconnect() {
        if (client == null) {
            return;
        }

        try {
            if (client.isConnected()) {
                client.disconnect(5000);
            }
        } catch (MqttException e) {
            System.out.println("Cannot disconnect Administration Server from MQTT: " + e.getMessage());
        } finally {
            closeClient();
        }
    }

    private void closeClient() {
        if (client == null) {
            return;
        }

        try {
            client.close();
        } catch (MqttException e) {
            System.out.println("Cannot close Administration Server MQTT client: " + e.getMessage());
        } finally {
            client = null;
        }
    }
}
