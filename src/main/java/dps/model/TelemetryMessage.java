package dps.model;

import java.util.ArrayList;
import java.util.List;

// Carries an MQTT batch of vibration averages and the line's current operating state.
public class TelemetryMessage {
    private Integer productionLineId;
    private ArrayList<Double> averages;
    private long timestamp;
    private OperationalState currentState;

    public TelemetryMessage() {
        averages = new ArrayList<>();
    }

    public TelemetryMessage(int productionLineId, List<Double> averages,
            long timestamp, OperationalState currentState) {
        this.productionLineId = productionLineId;
        this.averages = new ArrayList<>(averages);
        this.timestamp = timestamp;
        this.currentState = currentState;
    }

    public Integer getProductionLineId() {
        return productionLineId;
    }

    public void setProductionLineId(Integer productionLineId) {
        this.productionLineId = productionLineId;
    }

    public ArrayList<Double> getAverages() {
        return averages;
    }

    public void setAverages(ArrayList<Double> averages) {
        this.averages = averages;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public OperationalState getCurrentState() {
        return currentState;
    }

    public void setCurrentState(OperationalState currentState) {
        this.currentState = currentState;
    }
}
