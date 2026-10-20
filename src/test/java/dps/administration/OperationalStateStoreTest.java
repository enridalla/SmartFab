package dps.administration;

import dps.model.OperationalState;
import dps.model.StateUpdateMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Verifies state initialization, update ordering, and cleanup across registrations.
class OperationalStateStoreTest {
    @Test
    void registrationTimestampRejectsStateFromAnOlderIncarnation() {
        OperationalStateStore store = new OperationalStateStore();
        store.addProductionLine(1, 100);

        assertTrue(store.updateState(new StateUpdateMessage(
                1, OperationalState.UnderCalibration, 99, 0.5)));
        assertEquals(OperationalState.FullyOperational, store.getState(1));

        assertTrue(store.updateState(new StateUpdateMessage(
                1, OperationalState.WaitingForCalibration, 100, 0.5)));
        assertEquals(OperationalState.WaitingForCalibration, store.getState(1));
    }

    @Test
    void registrationInitializesAndUpdatesStatesIndependently() {
        OperationalStateStore store = new OperationalStateStore();
        store.addProductionLine(1);
        store.addProductionLine(2);

        assertEquals(OperationalState.FullyOperational, store.getState(1));
        assertEquals(OperationalState.FullyOperational, store.getState(2));

        assertTrue(store.updateState(message(1, OperationalState.WaitingForCalibration, 100)));
        assertEquals(OperationalState.WaitingForCalibration, store.getState(1));
        assertEquals(OperationalState.FullyOperational, store.getState(2));
    }

    @Test
    void olderUpdatesDoNotOverwriteNewerStates() {
        OperationalStateStore store = new OperationalStateStore();
        store.addProductionLine(1);

        store.updateState(message(1, OperationalState.UnderCalibration, 200));
        store.updateState(message(1, OperationalState.WaitingForCalibration, 100));

        assertEquals(OperationalState.UnderCalibration, store.getState(1));
    }

    @Test
    void removalRejectsLateUpdatesAndRejoinStartsFullyOperational() {
        OperationalStateStore store = new OperationalStateStore();
        store.addProductionLine(1);
        store.updateState(message(1, OperationalState.UnderCalibration, 200));

        assertTrue(store.removeProductionLine(1));
        assertFalse(store.updateState(message(1, OperationalState.FullyOperational, 300)));
        assertNull(store.getState(1));

        store.addProductionLine(1);
        assertEquals(OperationalState.FullyOperational, store.getState(1));
    }

    private StateUpdateMessage message(int productionLineId,
            OperationalState state,
            long timestamp) {
        Double criticality = state == OperationalState.FullyOperational ? null : 0.25;
        return new StateUpdateMessage(productionLineId, state, timestamp, criticality);
    }
}
