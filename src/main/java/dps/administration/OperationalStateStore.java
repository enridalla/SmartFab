package dps.administration;

import dps.model.OperationalState;
import dps.model.ProductionLineState;
import dps.model.StateUpdateMessage;
import org.springframework.stereotype.Service;

import java.util.ArrayList;

// Stores the latest operational state of each registered production line.
@Service
public class OperationalStateStore {
    private final ArrayList<StateEntry> states = new ArrayList<>();

    public synchronized void addProductionLine(int productionLineId) {
        addProductionLine(productionLineId, 0);
    }

    synchronized void addProductionLine(int productionLineId, long acceptedFromTimestamp) {
        StateEntry entry = findState(productionLineId);

        if (entry == null) {
            states.add(new StateEntry(productionLineId, acceptedFromTimestamp));
        } else {
            entry.operationalState = OperationalState.FullyOperational;
            entry.timestamp = acceptedFromTimestamp;
        }
    }

    public synchronized boolean removeProductionLine(int productionLineId) {
        for (int i = 0; i < states.size(); i++) {
            if (states.get(i).productionLineId == productionLineId) {
                states.remove(i);
                return true;
            }
        }

        return false;
    }

    public synchronized boolean updateState(StateUpdateMessage message) {
        StateEntry entry = findState(message.getProductionLineId());

        if (entry == null) {
            return false;
        }

        // Ignore older updates without treating a registered line as inactive.
        if (message.getTimestamp() < entry.timestamp) {
            return true;
        }

        entry.operationalState = message.getNewState();
        entry.timestamp = message.getTimestamp();
        return true;
    }

    public synchronized ArrayList<ProductionLineState> getStates() {
        // Copy mutable entries so REST callers cannot change the stored states.
        ArrayList<ProductionLineState> copy = new ArrayList<>();

        for (StateEntry entry : states) {
            copy.add(new ProductionLineState(entry.productionLineId, entry.operationalState));
        }

        return copy;
    }

    synchronized OperationalState getState(int productionLineId) {
        StateEntry entry = findState(productionLineId);
        if (entry == null) {
            return null;
        }

        return entry.operationalState;
    }

    private StateEntry findState(int productionLineId) {
        for (StateEntry entry : states) {
            if (entry.productionLineId == productionLineId) {
                return entry;
            }
        }

        return null;
    }

    // Tracks the timestamp used to reject updates older than the current state.
    private static class StateEntry {
        private final int productionLineId;
        private OperationalState operationalState;
        private long timestamp;

        private StateEntry(int productionLineId, long acceptedFromTimestamp) {
            this.productionLineId = productionLineId;
            this.operationalState = OperationalState.FullyOperational;
            this.timestamp = acceptedFromTimestamp;
        }
    }
}
