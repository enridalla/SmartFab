package dps.productionline;

import dps.model.ProductionLineInfo;

import java.util.ArrayList;

// Maintains the local peer view and reconciles membership changes during registration.
public class PeerList {
    private final int ownId;
    private final ArrayList<ProductionLineInfo> peers = new ArrayList<>();
    private final ArrayList<Integer> leftDuringRegistration = new ArrayList<>();
    private boolean registrationPending = true;

    public PeerList(int ownId) {
        this.ownId = ownId;
    }

    public synchronized boolean addPeer(ProductionLineInfo peer) {
        // A production line must never add itself or a peer without a complete endpoint.
        if (!isValid(peer) || peer.getId() == ownId) {
            return false;
        }

        // IDs identify peers throughout the distributed protocol, so each ID appears once.
        for (ProductionLineInfo current : peers) {
            if (current.getId().equals(peer.getId())) {
                return false;
            }
        }

        // Keep an independent copy because ProductionLineInfo can be changed by its caller.
        peers.add(new ProductionLineInfo(peer));
        return true;
    }

    public synchronized ArrayList<ProductionLineInfo> getPeers() {
        ArrayList<ProductionLineInfo> copy = new ArrayList<>();

        // This deep snapshot can be iterated while other threads discover new peers.
        for (ProductionLineInfo peer : peers) {
            copy.add(new ProductionLineInfo(peer));
        }

        return copy;
    }

    public synchronized ArrayList<ProductionLineInfo> mergeRegistrationSnapshot(ProductionLineInfo[] snapshot) {
        ArrayList<ProductionLineInfo> added = new ArrayList<>();
        for (ProductionLineInfo peer : snapshot) {
            // A leave received while REST was replying is newer than its snapshot.
            if (isValid(peer) && !leftDuringRegistration.contains(peer.getId()) && addPeer(peer)) {
                added.add(new ProductionLineInfo(peer));
            }
        }
        registrationPending = false;
        leftDuringRegistration.clear();
        return added;
    }

    public synchronized boolean removePeer(int peerId) {
        if (registrationPending && !leftDuringRegistration.contains(peerId)) {
            leftDuringRegistration.add(peerId);
        }
        for (int i = 0; i < peers.size(); i++) {
            if (peers.get(i).getId() == peerId) {
                peers.remove(i);
                return true;
            }
        }

        return false;
    }

    public synchronized boolean containsPeer(int peerId) {
        for (ProductionLineInfo peer : peers) {
            if (peer.getId() == peerId) {
                return true;
            }
        }

        return false;
    }

    public synchronized String getPeerIds() {
        StringBuilder result = new StringBuilder("[");

        // Build the diagnostic text while holding the same lock used to update the list.
        for (int i = 0; i < peers.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }

            result.append(peers.get(i).getId());
        }

        result.append("]");
        return result.toString();
    }

    private boolean isValid(ProductionLineInfo peer) {
        // Every accepted peer needs enough information to attempt a gRPC connection.
        return peer != null
                && peer.getId() != null
                && peer.getAddress() != null
                && !peer.getAddress().isBlank()
                && peer.getPort() > 0
                && peer.getPort() <= 65535;
    }
}
