package dps.administration;

import dps.model.ProductionLineInfo;
import dps.model.ProductionLineState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

// Exposes REST endpoints for line membership, operational states and vibration averages.
@RestController
@RequestMapping("/production-lines")
public class ProductionLineController {
    private final ProductionLineRegistry registry;
    private final TelemetryStore telemetryStore;
    private final OperationalStateStore stateStore;

    @Autowired
    public ProductionLineController(ProductionLineRegistry registry,
            TelemetryStore telemetryStore,
            OperationalStateStore stateStore) {
        this.registry = registry;
        this.telemetryStore = telemetryStore;
        this.stateStore = stateStore;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public synchronized ResponseEntity<List<ProductionLineInfo>> register(
            @RequestBody ProductionLineInfo productionLine) {
        // Reject incomplete identities before they reach the shared registry.
        if (!isValid(productionLine)) {
            return ResponseEntity.badRequest().build();
        }

        // A successful registration returns the lines that were already registered.
        ArrayList<ProductionLineInfo> peers = registry.register(productionLine);

        // The registry uses null to report that another line already owns this ID.
        if (peers == null) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }

        int productionLineId = productionLine.getId();
        // Compare event timestamps with the same production line clock,
        // since another machine's clock can differ from the administration server's clock.
        Long suppliedTimestamp = productionLine.getRegistrationTimestamp();
        long registrationTimestamp = suppliedTimestamp == null ? 0 : suppliedTimestamp;
        telemetryStore.addProductionLine(productionLineId, registrationTimestamp);
        stateStore.addProductionLine(productionLineId, registrationTimestamp);

        // The new line uses this list to discover and contact its existing peers.
        return ResponseEntity.status(HttpStatus.CREATED).body(peers);
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<ProductionLineInfo> getProductionLines() {
        // The registry returns a snapshot, so serializing the response cannot change its state.
        return registry.getProductionLines();
    }

    @GetMapping(value = "/states", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<ProductionLineState> getProductionLineStates() {
        return stateStore.getStates();
    }

    @GetMapping(value = "/{id}/average", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Double> getAverage(@PathVariable int id,
            @RequestParam long t1,
            @RequestParam long t2) {
        if (t1 > t2) {
            return ResponseEntity.badRequest().build();
        }

        ArrayList<Double> values = telemetryStore.readValuesInRange(id, t1, t2);
        if (values == null) {
            return ResponseEntity.notFound().build();
        }

        Double average = TelemetryStore.averageOf(values);
        if (average == null) {
            return ResponseEntity.noContent().build();
        }

        return ResponseEntity.ok(average);
    }

    @DeleteMapping("/{id}")
    public synchronized ResponseEntity<Void> remove(@PathVariable int id) {
        if (!registry.remove(id)) {
            return ResponseEntity.notFound().build();
        }

        telemetryStore.removeProductionLine(id);
        stateStore.removeProductionLine(id);
        return ResponseEntity.noContent().build();
    }

    private boolean isValid(ProductionLineInfo productionLine) {
        // Addresses must be present and ports must fall in the valid network range.
        return productionLine != null
                && productionLine.getId() != null
                && productionLine.getAddress() != null
                && !productionLine.getAddress().isBlank()
                && productionLine.getPort() > 0
                && productionLine.getPort() <= 65535
                && (productionLine.getRegistrationTimestamp() == null
                    || productionLine.getRegistrationTimestamp() >= 0);
    }
}
