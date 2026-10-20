package dps.administration;

import dps.model.ProductionLineInfo;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Verifies unique registration, concurrent duplicate rejection, and production line removal.
class ProductionLineRegistryTest {
    @Test
    void registrationReturnsOnlyPreviousLinesAndRejectsDuplicates() {
        ProductionLineRegistry registry = new ProductionLineRegistry();

        ArrayList<ProductionLineInfo> firstPeers = registry.register(new ProductionLineInfo(1, "127.0.0.1", 50051));
        ArrayList<ProductionLineInfo> secondPeers = registry.register(new ProductionLineInfo(200, "127.0.0.1", 50052));

        assertNotNull(firstPeers);
        assertEquals(0, firstPeers.size());
        assertNotNull(secondPeers);
        assertEquals(1, secondPeers.size());
        assertEquals(1, secondPeers.get(0).getId());
        assertNull(registry.register(new ProductionLineInfo(200, "127.0.0.1", 50062)));
        assertEquals(2, registry.getProductionLines().size());
    }

    @Test
    void concurrentRegistrationsCannotInsertTheSameIdTwice() throws InterruptedException {
        ProductionLineRegistry registry = new ProductionLineRegistry();
        Thread[] threads = new Thread[20];
        boolean[] successful = new boolean[20];

        for (int i = 0; i < threads.length; i++) {
            final int index = i;
            threads[i] = new Thread(() -> successful[index] = registry.register(
                    new ProductionLineInfo(7, "127.0.0.1", 50100 + index)) != null);
        }

        // Start every worker before waiting so the registration attempts can overlap.
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait until every attempt has recorded its result before checking the outcome.
        for (Thread thread : threads) {
            thread.join();
        }

        // Only one thread should win the duplicate-ID registration race.
        int successfulRegistrations = 0;
        for (boolean result : successful) {
            if (result) {
                successfulRegistrations++;
            }
        }

        assertEquals(1, successfulRegistrations);
        assertEquals(1, registry.getProductionLines().size());
    }

    @Test
    void removedIdCanRegisterAgainWithoutRemovingOtherLines() {
        ProductionLineRegistry registry = new ProductionLineRegistry();
        registry.register(new ProductionLineInfo(1, "127.0.0.1", 50051));
        registry.register(new ProductionLineInfo(2, "127.0.0.1", 50052));

        assertTrue(registry.contains(1));
        assertTrue(registry.remove(1));
        assertFalse(registry.contains(1));
        assertFalse(registry.remove(1));
        assertEquals(1, registry.getProductionLines().size());
        assertEquals(2, registry.getProductionLines().get(0).getId());

        assertNotNull(registry.register(new ProductionLineInfo(1, "127.0.0.1", 50061)));
        assertTrue(registry.contains(1));
    }
}
