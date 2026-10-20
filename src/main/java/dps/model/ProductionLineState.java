package dps.model;

// Represents a line's current operating state returned by the administration REST API.
public class ProductionLineState {
    private Integer productionLineId;
    private OperationalState operationalState;

    public ProductionLineState() {
    }

    public ProductionLineState(int productionLineId, OperationalState operationalState) {
        this.productionLineId = productionLineId;
        this.operationalState = operationalState;
    }

    public Integer getProductionLineId() {
        return productionLineId;
    }

    public void setProductionLineId(Integer productionLineId) {
        this.productionLineId = productionLineId;
    }

    public OperationalState getOperationalState() {
        return operationalState;
    }

    public void setOperationalState(OperationalState operationalState) {
        this.operationalState = operationalState;
    }
}
