package dps.model;

// Carries an MQTT state transition with its occurrence time and optional vibration criticality.
public class StateUpdateMessage {
    private Integer productionLineId;
    private OperationalState newState;
    private long timestamp;
    private Double criticality;

    public StateUpdateMessage() {
    }

    public StateUpdateMessage(int productionLineId, OperationalState newState,
            long timestamp, Double criticality) {
        this.productionLineId = productionLineId;
        this.newState = newState;
        this.timestamp = timestamp;
        this.criticality = criticality;
    }

    public Integer getProductionLineId() {
        return productionLineId;
    }

    public void setProductionLineId(Integer productionLineId) {
        this.productionLineId = productionLineId;
    }

    public OperationalState getNewState() {
        return newState;
    }

    public void setNewState(OperationalState newState) {
        this.newState = newState;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getCriticality() {
        return criticality;
    }

    public void setCriticality(Double criticality) {
        this.criticality = criticality;
    }
}
