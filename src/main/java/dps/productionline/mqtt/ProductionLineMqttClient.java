package dps.productionline.mqtt;

import dps.model.OperationalState;
import dps.model.StateUpdateMessage;
import dps.model.TelemetryMessage;
import dps.mqtt.MqttConfiguration;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

// Publishes production line telemetry and operational state changes through MQTT.
public class ProductionLineMqttClient implements MqttCallback {
    private final int productionLineId;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private MqttClient client;

    public ProductionLineMqttClient(int productionLineId) {
        this.productionLineId = productionLineId;
    }

    public synchronized boolean connect() {
        try {
            client = new MqttClient(
                    MqttConfiguration.BROKER_ADDRESS,
                    MqttClient.generateClientId(),
                    new MemoryPersistence());
            client.setTimeToWait(5000);
            client.setCallback(this);

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setConnectionTimeout(5);

            client.connect(options);
            System.out.println("Production Line " + productionLineId + " connected to MQTT broker");
            return true;
        } catch (MqttException | IllegalArgumentException e) {
            System.out.println("Production Line " + productionLineId
                    + " cannot connect to MQTT broker " + MqttConfiguration.BROKER_ADDRESS
                    + ": " + e.getMessage());
            closeClient();
            return false;
        }
    }

    public boolean publishTelemetry(TelemetryMessage message) {
        return publish(MqttConfiguration.TELEMETRY_TOPIC, message);
    }

    public boolean publishStateUpdate(OperationalState state, long timestamp, Double criticality) {
        StateUpdateMessage message = new StateUpdateMessage(
                productionLineId,
                state,
                timestamp,
                criticality);
        return publish(MqttConfiguration.STATE_TOPIC, message);
    }

    // Share the lifecycle lock so disconnect cannot close the client during publication.
    private synchronized boolean publish(String topic, Object payload) {
        if (client == null || !client.isConnected()) {
            System.out.println("Production Line " + productionLineId
                    + " cannot publish on " + topic + ": MQTT is not connected");
            return false;
        }

        try {
            String json = objectMapper.writeValueAsString(payload);
            MqttMessage message = new MqttMessage(json.getBytes(StandardCharsets.UTF_8));
            message.setQos(MqttConfiguration.QOS);
            message.setRetained(false);
            client.publish(topic, message);
            return true;
        } catch (Exception e) {
            System.out.println("Production Line " + productionLineId
                    + " cannot publish on " + topic + ": " + e.getMessage());
            return false;
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
    }

    @Override
    public void connectionLost(Throwable cause) {
        String message = cause == null ? "unknown cause" : cause.getMessage();
        System.out.println("Production Line " + productionLineId
                + " lost the MQTT connection: " + message);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    public synchronized void disconnect() {
        if (client == null) {
            return;
        }

        try {
            if (client.isConnected()) {
                client.disconnect();
            }
        } catch (MqttException e) {
            System.out.println("Production Line " + productionLineId
                    + " cannot disconnect from MQTT: " + e.getMessage());
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
            System.out.println("Production Line " + productionLineId
                    + " cannot close its MQTT client: " + e.getMessage());
        } finally {
            client = null;
        }
    }
}
