package dps.productionline;

import dps.model.ProductionLineInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Verifies peer uniqueness, removal, and registration snapshots after a peer leaves.
class PeerListTest {
    @Test
    void peerListRejectsTheLocalLineAndDuplicateIds() {
        PeerList peerList = new PeerList(1);

        assertFalse(peerList.addPeer(new ProductionLineInfo(1, "127.0.0.1", 50051)));
        assertTrue(peerList.addPeer(new ProductionLineInfo(200, "127.0.0.1", 50052)));
        assertFalse(peerList.addPeer(new ProductionLineInfo(200, "127.0.0.1", 50062)));
        assertEquals("[200]", peerList.getPeerIds());
        assertEquals(1, peerList.getPeers().size());
    }

    @Test
    void removePeerIsIdempotentAndDoesNotRemoveOtherPeers() {
        PeerList peerList = new PeerList(1);
        peerList.addPeer(new ProductionLineInfo(2, "127.0.0.1", 50052));
        peerList.addPeer(new ProductionLineInfo(3, "127.0.0.1", 50053));

        assertTrue(peerList.containsPeer(2));
        assertTrue(peerList.removePeer(2));
        assertFalse(peerList.containsPeer(2));
        assertFalse(peerList.removePeer(2));
        assertEquals("[3]", peerList.getPeerIds());
    }

    @Test
    void registrationSnapshotCannotResurrectAPeerWhoseLeaveAlreadyArrived() {
        PeerList peerList = new PeerList(4);
        peerList.addPeer(new ProductionLineInfo(5, "localhost", 50055));
        peerList.removePeer(2);

        var added = peerList.mergeRegistrationSnapshot(new ProductionLineInfo[]{
                new ProductionLineInfo(1, "localhost", 50051),
                new ProductionLineInfo(2, "localhost", 50052),
                new ProductionLineInfo(5, "localhost", 50055)
        });

        assertEquals(1, added.size());
        assertEquals(1, added.get(0).getId());
        assertFalse(peerList.containsPeer(2));
        assertTrue(peerList.containsPeer(1));
        assertTrue(peerList.containsPeer(5));
        // A later explicit introduction can reuse an ID after a completed leave.
        assertTrue(peerList.addPeer(new ProductionLineInfo(2, "localhost", 50062)));
    }
}
