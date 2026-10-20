package dps.productionline.calibration;

import dps.model.ProductionLineInfo;
import dps.productionline.PeerList;
import dps.productionline.ProductionLineServiceOuterClass;
import dps.productionline.grpc.ProductionLineServiceImpl;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Verifies peer departure cleanup and its synchronization with calibration request acceptance.
class ProductionLineServiceLeaveTest {
    @Test
    void leaveRemovesPeerAndCoordinatorRecordsAndIsIdempotent() {
        PeerList peerList = new PeerList(1);
        peerList.addPeer(new ProductionLineInfo(2, "127.0.0.1", 50052));
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, peerList);
        coordinator.handleCalibrationRequest(2, 7, 0.2);
        assertEquals(1, coordinator.getGrantedRequestCount());

        ProductionLineServiceImpl service = new ProductionLineServiceImpl(peerList, coordinator);
        ProductionLineServiceOuterClass.LeaveRequest request = ProductionLineServiceOuterClass.LeaveRequest
                .newBuilder()
                .setProductionLineId(2)
                .build();
        ResponseObserver<ProductionLineServiceOuterClass.LeaveResponse> first = new ResponseObserver<>();
        ResponseObserver<ProductionLineServiceOuterClass.LeaveResponse> duplicate = new ResponseObserver<>();

        service.leave(request, first);
        service.leave(request, duplicate);

        assertFalse(peerList.containsPeer(2));
        assertEquals(0, coordinator.getGrantedRequestCount());
        assertNotNull(first.value);
        assertTrue(first.completed);
        assertNotNull(duplicate.value);
        assertTrue(duplicate.completed);
    }

    @Test
    void calibrationRequestFromRemovedPeerIsRejectedWithoutCreatingRecords() {
        PeerList peerList = new PeerList(1);
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, peerList);
        ProductionLineServiceImpl service = new ProductionLineServiceImpl(peerList, coordinator);
        ResponseObserver<ProductionLineServiceOuterClass.CalibrationResponse> observer = new ResponseObserver<>();

        ProductionLineServiceOuterClass.CalibrationRequest request = ProductionLineServiceOuterClass.CalibrationRequest
                .newBuilder()
                .setRequesterId(2)
                .setRequestNumber(1)
                .setCriticality(0.5)
                .build();

        service.requestCalibration(request, observer);

        assertNotNull(observer.value);
        assertFalse(observer.value.getGranted());
        assertEquals(0, coordinator.getGrantedRequestCount());
        assertEquals(0, coordinator.getDeferredRequestCount());
        assertTrue(observer.completed);
    }

    @Test
    void leaveCleanupCannotFinishBetweenMembershipCheckAndRequestRecording() throws InterruptedException {
        PausingPeerList peerList = new PausingPeerList(1);
        peerList.addPeer(new ProductionLineInfo(2, "127.0.0.1", 50052));
        ObservingCalibrationCoordinator coordinator = new ObservingCalibrationCoordinator(1, peerList);
        ProductionLineServiceImpl service = new ProductionLineServiceImpl(peerList, coordinator);
        ResponseObserver<ProductionLineServiceOuterClass.CalibrationResponse> requestObserver = new ResponseObserver<>();
        ResponseObserver<ProductionLineServiceOuterClass.LeaveResponse> leaveObserver = new ResponseObserver<>();

        ProductionLineServiceOuterClass.CalibrationRequest calibrationRequest = ProductionLineServiceOuterClass.CalibrationRequest
                .newBuilder()
                .setRequesterId(2)
                .setRequestNumber(7)
                .setCriticality(0.2)
                .build();
        ProductionLineServiceOuterClass.LeaveRequest leaveRequest = ProductionLineServiceOuterClass.LeaveRequest
                .newBuilder()
                .setProductionLineId(2)
                .build();

        Thread requestThread = new Thread(() -> service.requestCalibration(calibrationRequest, requestObserver));
        Thread leaveThread = new Thread(() -> service.leave(leaveRequest, leaveObserver));

        try {
            requestThread.start();
            assertTrue(peerList.membershipWasRead.await(1, TimeUnit.SECONDS));

            leaveThread.start();
            // Membership removal and protocol cleanup use the same coordinator monitor
            // as request acceptance, so neither half of leave can overtake this request.
            assertFalse(peerList.peerWasRemoved.await(100, TimeUnit.MILLISECONDS));
            assertFalse(coordinator.peerLeftEntered.await(100, TimeUnit.MILLISECONDS));
        } finally {
            peerList.allowMembershipCheckToReturn.countDown();
            requestThread.join(1000);
            leaveThread.join(1000);
        }

        assertFalse(requestThread.isAlive());
        assertFalse(leaveThread.isAlive());
        assertFalse(peerList.containsPeer(2));
        assertEquals(0, coordinator.getGrantedRequestCount());
        assertEquals(0, coordinator.getDeferredRequestCount());
        assertNotNull(requestObserver.value);
        assertNotNull(leaveObserver.value);
        assertTrue(requestObserver.completed);
        assertTrue(leaveObserver.completed);
    }

    private static class ResponseObserver<T> implements StreamObserver<T> {
        private T value;
        private boolean completed;

        @Override
        public void onNext(T value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }

    private static class PausingPeerList extends PeerList {
        private final CountDownLatch membershipWasRead = new CountDownLatch(1);
        private final CountDownLatch allowMembershipCheckToReturn = new CountDownLatch(1);
        private final CountDownLatch peerWasRemoved = new CountDownLatch(1);

        private PausingPeerList(int ownId) {
            super(ownId);
        }

        @Override
        public boolean containsPeer(int peerId) {
            boolean contained = super.containsPeer(peerId);
            membershipWasRead.countDown();

            try {
                if (!allowMembershipCheckToReturn.await(2, TimeUnit.SECONDS)) {
                    throw new AssertionError("Membership check was not released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }

            return contained;
        }

        @Override
        public boolean removePeer(int peerId) {
            boolean removed = super.removePeer(peerId);
            peerWasRemoved.countDown();
            return removed;
        }
    }

    private static class ObservingCalibrationCoordinator extends CalibrationCoordinator {
        private final CountDownLatch peerLeftEntered = new CountDownLatch(1);

        private ObservingCalibrationCoordinator(int ownId, PeerList peerList) {
            super(ownId, peerList);
        }

        @Override
        public synchronized void peerLeft(int peerId) {
            peerLeftEntered.countDown();
            super.peerLeft(peerId);
        }
    }
}
