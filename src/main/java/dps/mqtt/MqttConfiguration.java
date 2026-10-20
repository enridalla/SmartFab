package dps.mqtt;

// Defines the MQTT broker, topics, and delivery quality shared by publishers and subscribers.
public final class MqttConfiguration {
    // Uses a local broker by default; override through the dps.mqtt.broker JVM property.
    public static final String BROKER_ADDRESS =
            System.getProperty("dps.mqtt.broker", "tcp://localhost:1883");
    public static final String TELEMETRY_TOPIC = "smartfab/telemetry";
    public static final String STATE_TOPIC = "smartfab/state";
    public static final int QOS = 2;

    private MqttConfiguration() {
    }
}
