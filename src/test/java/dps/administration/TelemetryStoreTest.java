package dps.administration;

import dps.model.OperationalState;
import dps.model.TelemetryMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Verifies telemetry lifecycle, inclusive averages, and concurrent storage access.
class TelemetryStoreTest {
    @Test
    void registrationTimestampRejectsTelemetryFromAnOlderIncarnation() {
        TelemetryStore store = new TelemetryStore();
        store.addProductionLine(1, 100);

        assertFalse(store.addTelemetry(message(1, 99, 50)));
        assertTrue(store.addTelemetry(message(1, 100, 60)));
        assertEquals(60.0, store.calculateAverage(1, 0, 200));
    }

    @Test
    void averageFlattensAllValuesFromBatchesInTheInclusiveRange() {
        TelemetryStore store = new TelemetryStore();
        store.addProductionLine(1);
        store.addProductionLine(2);

        assertTrue(store.addTelemetry(message(1, 100, 10, 20)));
        assertTrue(store.addTelemetry(message(1, 200, 100)));
        assertTrue(store.addTelemetry(message(1, 300, 1000)));
        assertTrue(store.addTelemetry(message(2, 200, 500)));

        assertEquals((10.0 + 20.0 + 100.0) / 3.0,
                store.calculateAverage(1, 100, 200));
        assertEquals(500.0, store.calculateAverage(2, 0, 1000));
    }

    @Test
    void emptyBatchesAndIntervalsReturnNoAverage() {
        TelemetryStore store = new TelemetryStore();
        store.addProductionLine(1);
        store.addTelemetry(message(1, 100));

        assertNull(store.calculateAverage(1, 0, 1000));
        assertNull(store.calculateAverage(1, 200, 300));
    }

    @Test
    void inactiveAndRemovedLinesCannotStoreTelemetry() {
        TelemetryStore store = new TelemetryStore();

        assertFalse(store.addTelemetry(message(4, 100, 10)));
        store.addProductionLine(4);
        assertTrue(store.addTelemetry(message(4, 100, 10)));
        assertEquals(1, store.getBatchCount(4));

        assertTrue(store.removeProductionLine(4));
        assertFalse(store.addTelemetry(message(4, 200, 20)));
        assertNull(store.calculateAverage(4, 0, 1000));
    }

    @Test
    void readsAndWritesCanRunConcurrently() throws InterruptedException {
        TelemetryStore store = new TelemetryStore();
        store.addProductionLine(1);

        Thread writer = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                store.addTelemetry(message(1, i, i));
            }
        });

        Thread reader = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                store.calculateAverage(1, 0, 100);
            }
        });

        writer.start();
        reader.start();
        writer.join();
        reader.join();

        assertEquals(49.5, store.calculateAverage(1, 0, 100));
    }

    private TelemetryMessage message(int productionLineId, long timestamp, double... values) {
        Double[] boxed = new Double[values.length];
        for (int i = 0; i < values.length; i++) {
            boxed[i] = values[i];
        }

        return new TelemetryMessage(
                productionLineId,
                List.of(boxed),
                timestamp,
                OperationalState.FullyOperational);
    }
}
