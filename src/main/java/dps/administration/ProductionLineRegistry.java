package dps.administration;

import dps.model.ProductionLineInfo;
import org.springframework.stereotype.Service;

import java.util.ArrayList;

// Maintains registered production lines and provides independent peer snapshots.
@Service
public class ProductionLineRegistry {
    private final ArrayList<ProductionLineInfo> productionLines = new ArrayList<>();

    public synchronized ArrayList<ProductionLineInfo> register(ProductionLineInfo productionLine) {
        // The synchronized method keeps the duplicate check and insertion atomic when
        // several REST requests arrive at the same time.
        for (ProductionLineInfo current : productionLines) {
            if (current.getId().equals(productionLine.getId())) {
                System.out.println("Registration rejected: line " + productionLine.getId() + " already exists");
                return null;
            }
        }

        // Take the snapshot before inserting the new line. The response must contain
        // only the peers that the new line needs to introduce itself to.
        ArrayList<ProductionLineInfo> peers = copyProductionLines();

        // Store a copy because the object received from the REST request is mutable.
        productionLines.add(new ProductionLineInfo(productionLine));
        System.out.println("Production Line " + productionLine.getId() + " registered");
        return peers;
    }

    public synchronized ArrayList<ProductionLineInfo> getProductionLines() {
        // Callers receive their own snapshot instead of the registry's mutable list.
        return copyProductionLines();
    }

    public synchronized boolean contains(int productionLineId) {
        for (ProductionLineInfo productionLine : productionLines) {
            if (productionLine.getId() == productionLineId) {
                return true;
            }
        }

        return false;
    }

    public synchronized boolean remove(int productionLineId) {
        for (int i = 0; i < productionLines.size(); i++) {
            if (productionLines.get(i).getId() == productionLineId) {
                productionLines.remove(i);
                System.out.println("Production Line " + productionLineId + " removed");
                return true;
            }
        }

        return false;
    }

    private ArrayList<ProductionLineInfo> copyProductionLines() {
        ArrayList<ProductionLineInfo> copy = new ArrayList<>();

        // Copy each mutable entry as well as the list itself, so callers cannot edit
        // an address, port, or ID stored inside the registry.
        for (ProductionLineInfo productionLine : productionLines) {
            copy.add(new ProductionLineInfo(productionLine));
        }

        return copy;
    }
}
