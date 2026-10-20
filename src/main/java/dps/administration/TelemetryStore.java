package dps.administration;

import dps.model.TelemetryMessage;
import org.springframework.stereotype.Service;

import java.util.ArrayList;

// Stores telemetry batches for registered lines and computes averages over time intervals.
@Service
public class TelemetryStore {
    private final ArrayList<ProductionLineTelemetry> productionLines = new ArrayList<>();

    public synchronized void addProductionLine(int productionLineId) {
        addProductionLine(productionLineId, 0);
    }

    synchronized void addProductionLine(int productionLineId, long acceptedFromTimestamp) {
        ProductionLineTelemetry telemetry = findProductionLine(productionLineId);

        if (telemetry == null) {
            productionLines.add(new ProductionLineTelemetry(
                    productionLineId,
                    acceptedFromTimestamp));
        } else {
            // Reusing an ID starts a new history with its own registration timestamp.
            telemetry.batches.clear();
            telemetry.acceptedFromTimestamp = acceptedFromTimestamp;
        }
    }

    public synchronized boolean removeProductionLine(int productionLineId) {
        for (int i = 0; i < productionLines.size(); i++) {
            if (productionLines.get(i).productionLineId == productionLineId) {
                productionLines.remove(i);
                return true;
            }
        }

        return false;
    }

    public synchronized boolean addTelemetry(TelemetryMessage message) {
        ProductionLineTelemetry telemetry = findProductionLine(message.getProductionLineId());

        // Reject messages from removed lines or from before the current registration.
        if (telemetry == null || message.getTimestamp() < telemetry.acceptedFromTimestamp) {
            return false;
        }

        telemetry.batches.add(new TelemetryBatch(
                message.getTimestamp(),
                message.getAverages()));
        return true;
    }

    synchronized ArrayList<Double> readValuesInRange(int productionLineId, long t1, long t2) {
        ArrayList<Double> values = new ArrayList<>();

        ProductionLineTelemetry telemetry = findProductionLine(productionLineId);
        if (telemetry == null) {
            return null;
        }

        // Select whole batches by publication time, including both interval endpoints.
        for (TelemetryBatch batch : telemetry.batches) {
            if (batch.timestamp >= t1 && batch.timestamp <= t2) {
                values.addAll(batch.averages);
            }
        }

        return values;
    }

    public Double calculateAverage(int productionLineId, long t1, long t2) {
        // Compute from a snapshot so summing values does not hold the store monitor.
        return averageOf(readValuesInRange(productionLineId, t1, t2));
    }

    static Double averageOf(ArrayList<Double> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        double sum = 0;
        for (Double value : values) {
            sum += value;
        }

        return sum / values.size();
    }

    synchronized int getBatchCount(int productionLineId) {
        ProductionLineTelemetry telemetry = findProductionLine(productionLineId);
        if (telemetry == null) {
            return 0;
        }

        return telemetry.batches.size();
    }

    private ProductionLineTelemetry findProductionLine(int productionLineId) {
        for (ProductionLineTelemetry telemetry : productionLines) {
            if (telemetry.productionLineId == productionLineId) {
                return telemetry;
            }
        }

        return null;
    }

    // Groups batches under the timestamp of the line's current registration.
    private static class ProductionLineTelemetry {
        private final int productionLineId;
        private final ArrayList<TelemetryBatch> batches = new ArrayList<>();
        private long acceptedFromTimestamp;

        private ProductionLineTelemetry(int productionLineId, long acceptedFromTimestamp) {
            this.productionLineId = productionLineId;
            this.acceptedFromTimestamp = acceptedFromTimestamp;
        }
    }

    // Keeps a published batch independent of the mutable message received over MQTT.
    private static class TelemetryBatch {
        private final long timestamp;
        private final ArrayList<Double> averages;

        private TelemetryBatch(long timestamp, ArrayList<Double> averages) {
            this.timestamp = timestamp;
            this.averages = new ArrayList<>(averages);
        }
    }
}
