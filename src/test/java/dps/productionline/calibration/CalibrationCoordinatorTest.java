package dps.productionline.calibration;

import dps.model.OperationalState;
import dps.model.ProductionLineInfo;
import dps.productionline.PeerList;
import dps.productionline.ProductionLineServiceGrpc;
import dps.productionline.ProductionLineServiceOuterClass;
import dps.productionline.grpc.ProductionLineServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Verifies calibration priority, permission tracking, transport failures, and worker shutdown.
class CalibrationCoordinatorTest {
    @Test
    void thresholdIsStrictAndCriticalityUsesTheRequiredFormula() {
        CalibrationCoordinator coordinator = coordinator(1);

        assertFalse(coordinator.detectCriticalCondition(80));
        assertEquals(OperationalState.FullyOperational, coordinator.getState());

        assertTrue(coordinator.detectCriticalCondition(96));
        assertEquals(OperationalState.WaitingForCalibration, coordinator.getState());
        assertEquals((96.0 - 80.0) / 80.0, coordinator.getCriticality());
    }

    @Test
    void higherCriticalityAndThenHigherIdHavePriority() {
        assertTrue(CalibrationCoordinator.hasPriority(0.4, 1, 0.2, 2));
        assertFalse(CalibrationCoordinator.hasPriority(0.2, 2, 0.4, 1));

        assertTrue(CalibrationCoordinator.hasPriority(0.4, 5, 0.4, 3));
        assertFalse(CalibrationCoordinator.hasPriority(0.4, 3, 0.4, 5));
    }

    @Test
    void higherCriticalityEntersBeforeTheDeferredLine() throws InterruptedException {
        PeerList firstPeers = new PeerList(1);
        PeerList secondPeers = new PeerList(2);
        firstPeers.addPeer(new ProductionLineInfo(2, "127.0.0.1", 50052));
        secondPeers.addPeer(new ProductionLineInfo(1, "127.0.0.1", 50051));

        CalibrationCoordinator first = new CalibrationCoordinator(1, firstPeers);
        CalibrationCoordinator second = new CalibrationCoordinator(2, secondPeers);
        first.detectCriticalCondition(112);
        second.detectCriticalCondition(96);
        long firstRequestNumber = first.getCurrentRequestNumber();
        long secondRequestNumber = second.getCurrentRequestNumber();
        first.beginCalibrationRequest();
        second.beginCalibrationRequest();

        // Line 1 has higher criticality, so it defers line 2 while line 2 grants line 1.
        assertFalse(first.handleCalibrationRequest(
                2, secondRequestNumber, second.getCriticality()));
        assertTrue(second.handleCalibrationRequest(
                1, firstRequestNumber, first.getCriticality()));
        first.permissionReceived(2, firstRequestNumber);

        // The grant lets line 1 enter calibration, while line 2 must keep waiting.
        assertTrue(first.waitForCalibrationAccess());
        assertEquals(OperationalState.UnderCalibration, first.getState());
        assertEquals(OperationalState.WaitingForCalibration, second.getState());

        // These calls simulate the deferred grant and release sent when line 1 finishes.
        ArrayList<CalibrationRequestInfo> deferred = first.finishCalibration();
        assertEquals(1, deferred.size());
        second.permissionReceived(1, secondRequestNumber);
        second.releaseReceived(1, firstRequestNumber);

        assertTrue(second.waitForCalibrationAccess());
        assertEquals(OperationalState.UnderCalibration, second.getState());
    }

    @Test
    void deferredRequestIsStoredAndClearedWhenCalibrationFinishes() throws InterruptedException {
        CalibrationCoordinator coordinator = coordinator(1);
        coordinator.detectCriticalCondition(96);
        coordinator.beginCalibrationRequest();
        assertTrue(coordinator.waitForCalibrationAccess());

        assertFalse(coordinator.handleCalibrationRequest(2, 7, 0.5));
        assertEquals(1, coordinator.getDeferredRequestCount());

        ArrayList<CalibrationRequestInfo> deferred = coordinator.finishCalibration();

        assertEquals(1, deferred.size());
        assertEquals(0, coordinator.getDeferredRequestCount());
        assertEquals(1, coordinator.getGrantedRequestCount());
        assertEquals(OperationalState.FullyOperational, coordinator.getState());

        coordinator.releaseReceived(2, 7);
        assertEquals(0, coordinator.getGrantedRequestCount());
    }

    @Test
    void aLineDoesNotStartARequestWhileAnEarlierGrantIsOutstanding() throws InterruptedException {
        CalibrationCoordinator coordinator = coordinator(1);
        assertTrue(coordinator.handleCalibrationRequest(2, 9, 0.1));
        assertEquals(1, coordinator.getGrantedRequestCount());
        coordinator.detectCriticalCondition(96);

        Thread requestThread = new Thread(() -> {
            try {
                coordinator.beginCalibrationRequest();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        requestThread.start();
        Thread.sleep(100);

        assertTrue(requestThread.isAlive());
        assertFalse(coordinator.isRequestStarted());

        coordinator.releaseReceived(2, 9);
        requestThread.join(1000);

        assertFalse(requestThread.isAlive());
        assertTrue(coordinator.isRequestStarted());
    }

    @Test
    void aPeerJoiningDuringARequestBecomesARequiredPermission() throws InterruptedException {
        PeerList peers = new PeerList(1);
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, peers);
        coordinator.detectCriticalCondition(96);
        long requestNumber = coordinator.getCurrentRequestNumber();
        coordinator.beginCalibrationRequest();

        ProductionLineInfo peer = new ProductionLineInfo(2, "127.0.0.1", 50052);
        peers.addPeer(peer);

        CalibrationRequestThread request = coordinator.requestFromNewPeer(peer);

        assertNotNull(request);
        assertEquals(1, coordinator.getMissingPermissionCount());

        coordinator.permissionReceived(2, requestNumber);
        assertTrue(coordinator.waitForCalibrationAccess());
        assertEquals(OperationalState.UnderCalibration, coordinator.getState());
    }

    @Test
    void aPendingLineWithAnOutstandingGrantDoesNotCreateACircularWait() {
        CalibrationCoordinator coordinator = coordinator(1);

        assertTrue(coordinator.handleCalibrationRequest(2, 1, 0.1));
        assertTrue(coordinator.detectCriticalCondition(112));

        // Line 1 has higher priority than line 3, but cannot start its own request
        // while line 2 still holds its grant. It must grant line 3 as well.
        assertTrue(coordinator.handleCalibrationRequest(3, 1, 0.3));
        assertEquals(2, coordinator.getGrantedRequestCount());
        assertEquals(0, coordinator.getDeferredRequestCount());
    }

    @Test
    void aWaitingLineDoesNotDeferBeforeItsRequestHasBeenBroadcast() {
        CalibrationCoordinator coordinator = coordinator(1);

        assertTrue(coordinator.detectCriticalCondition(112));
        assertFalse(coordinator.isRequestStarted());

        // The local request has higher priority, but it is not yet active.
        assertTrue(coordinator.handleCalibrationRequest(2, 1, 0.1));
        assertEquals(1, coordinator.getGrantedRequestCount());
        assertEquals(0, coordinator.getDeferredRequestCount());
    }

    @Test
    void aGrantFromAnOldRoundDoesNotSatisfyTheCurrentRequest() throws InterruptedException {
        PeerList peers = new PeerList(1);
        peers.addPeer(new ProductionLineInfo(2, "127.0.0.1", 50052));
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, peers);

        coordinator.detectCriticalCondition(96);
        long firstRequestNumber = coordinator.getCurrentRequestNumber();
        coordinator.beginCalibrationRequest();
        coordinator.permissionReceived(2, firstRequestNumber);
        assertTrue(coordinator.waitForCalibrationAccess());
        coordinator.finishCalibration();
        coordinator.calibrationWorkerFinished();

        coordinator.detectCriticalCondition(96);
        long secondRequestNumber = coordinator.getCurrentRequestNumber();
        coordinator.beginCalibrationRequest();

        coordinator.permissionReceived(2, firstRequestNumber);
        assertEquals(1, coordinator.getMissingPermissionCount());

        coordinator.permissionReceived(2, secondRequestNumber);
        assertEquals(0, coordinator.getMissingPermissionCount());
    }

    @Test
    void aReleaseFromAnOldRoundDoesNotClearANewerOutstandingGrant() {
        CalibrationCoordinator coordinator = coordinator(1);

        assertTrue(coordinator.handleCalibrationRequest(2, 1, 0.1));
        coordinator.releaseReceived(2, 1);
        assertEquals(0, coordinator.getGrantedRequestCount());

        assertTrue(coordinator.handleCalibrationRequest(2, 2, 0.1));
        coordinator.releaseReceived(2, 1);
        assertEquals(1, coordinator.getGrantedRequestCount());

        coordinator.releaseReceived(2, 2);
        assertEquals(0, coordinator.getGrantedRequestCount());
    }

    @Test
    void exitCancelsAPendingRequestAndPreventsCalibrationEntry() throws InterruptedException {
        PeerList peers = new PeerList(1);
        peers.addPeer(new ProductionLineInfo(2, "127.0.0.1", 50052));
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, peers);

        assertTrue(coordinator.detectCriticalCondition(96));
        coordinator.beginCalibrationRequest();
        assertEquals(1, coordinator.getMissingPermissionCount());

        coordinator.requestExit();

        assertTrue(coordinator.isExitRequested());
        assertFalse(coordinator.isRequestStarted());
        assertEquals(0, coordinator.getCurrentRequestNumber());
        assertEquals(0, coordinator.getMissingPermissionCount());
        assertFalse(coordinator.waitForCalibrationAccess());

        coordinator.permissionReceived(2, 1);
        assertEquals(OperationalState.WaitingForCalibration, coordinator.getState());
    }

    @Test
    void peerLeaveRemovesAMissingPermission() throws InterruptedException {
        PeerList peers = new PeerList(1);
        peers.addPeer(new ProductionLineInfo(2, "127.0.0.1", 50052));
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, peers);

        coordinator.detectCriticalCondition(96);
        coordinator.beginCalibrationRequest();
        assertEquals(1, coordinator.getMissingPermissionCount());

        coordinator.peerLeft(2);

        assertEquals(0, coordinator.getMissingPermissionCount());
        assertTrue(coordinator.waitForCalibrationAccess());
    }

    @Test
    void peerLeaveClearsDeferredAndGrantedRequests() throws InterruptedException {
        CalibrationCoordinator grantedCoordinator = coordinator(1);
        assertTrue(grantedCoordinator.handleCalibrationRequest(2, 1, 0.2));
        assertTrue(grantedCoordinator.handleCalibrationRequest(2, 2, 0.3));
        assertEquals(2, grantedCoordinator.getGrantedRequestCount());

        grantedCoordinator.peerLeft(2);
        assertEquals(0, grantedCoordinator.getGrantedRequestCount());

        CalibrationCoordinator deferredCoordinator = coordinator(1);
        deferredCoordinator.detectCriticalCondition(96);
        deferredCoordinator.beginCalibrationRequest();
        deferredCoordinator.waitForCalibrationAccess();
        assertFalse(deferredCoordinator.handleCalibrationRequest(2, 3, 0.5));
        assertEquals(1, deferredCoordinator.getDeferredRequestCount());

        deferredCoordinator.peerLeft(2);
        deferredCoordinator.peerLeft(2);
        assertEquals(0, deferredCoordinator.getDeferredRequestCount());
    }

    @Test
    void exitWaitsForTheCompleteCalibrationWorkerLifecycle() throws InterruptedException {
        CalibrationCoordinator coordinator = coordinator(1);
        coordinator.detectCriticalCondition(96);
        coordinator.requestExit();

        Thread exitWaiter = new Thread(() -> {
            try {
                coordinator.waitUntilReadyToExit();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        exitWaiter.start();
        Thread.sleep(100);
        assertTrue(exitWaiter.isAlive());

        coordinator.calibrationWorkerFinished();
        exitWaiter.join(1000);
        assertFalse(exitWaiter.isAlive());
    }

    @Test
    void exitRejectsPeersThatTryToJoinAfterShutdownStarts() {
        PeerList peers = new PeerList(1);
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, peers);
        ProductionLineInfo latePeer = new ProductionLineInfo(2, "127.0.0.1", 50052);

        coordinator.requestExit();

        assertFalse(coordinator.addPeerIfActive(latePeer));
        assertFalse(peers.containsPeer(2));
    }

    @Test
    void exitWaitsForAnAdditionalCalibrationRequestWorker() throws InterruptedException {
        PeerList peers = new PeerList(1);
        peers.addPeer(new ProductionLineInfo(2, "127.0.0.1", 50052));
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, peers);
        coordinator.detectCriticalCondition(96);
        coordinator.beginCalibrationRequest();
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        CountDownLatch exitCompleted = new CountDownLatch(1);
        BlockingAdditionalRequestThread worker = new BlockingAdditionalRequestThread(
                coordinator,
                workerStarted,
                releaseWorker);

        coordinator.startAdditionalCalibrationRequest(worker);
        assertTrue(workerStarted.await(1, TimeUnit.SECONDS));
        coordinator.requestExit();
        coordinator.calibrationWorkerFinished();

        Thread exitWaiter = new Thread(() -> {
            try {
                coordinator.waitUntilReadyToExit();
                exitCompleted.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        try {
            exitWaiter.start();
            assertFalse(exitCompleted.await(100, TimeUnit.MILLISECONDS));

            releaseWorker.countDown();

            assertTrue(exitCompleted.await(1, TimeUnit.SECONDS));
        } finally {
            releaseWorker.countDown();
            worker.join(1000);
            exitWaiter.join(1000);
        }

        assertFalse(worker.isAlive());
        assertFalse(exitWaiter.isAlive());
    }

    @Test
    void exitPreventsAnAdditionalCalibrationRequestWorkerFromStarting() throws InterruptedException {
        CalibrationCoordinator coordinator = coordinator(1);
        BlockingAdditionalRequestThread worker = new BlockingAdditionalRequestThread(
                coordinator,
                new CountDownLatch(1),
                new CountDownLatch(1));

        coordinator.requestExit();
        coordinator.startAdditionalCalibrationRequest(worker);
        coordinator.waitUntilReadyToExit();

        assertEquals(Thread.State.NEW, worker.getState());
    }

    @Test
    void cancelledRequestBroadcastDoesNotStartItsWorkers() throws InterruptedException {
        CalibrationCoordinator coordinator = coordinator(1);
        coordinator.detectCriticalCondition(96);
        long requestNumber = coordinator.getCurrentRequestNumber();
        coordinator.beginCalibrationRequest();
        ArrayList<Thread> workers = new ArrayList<>();
        workers.add(new Thread());

        coordinator.requestExit();

        assertFalse(coordinator.startCalibrationRequestThreads(workers, requestNumber));
        assertEquals(Thread.State.NEW, workers.get(0).getState());
    }

    @Test
    void departedPeerCannotStartALateAdditionalRequest() throws InterruptedException {
        PeerList peers = new PeerList(1);
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, peers);
        coordinator.detectCriticalCondition(96);
        coordinator.beginCalibrationRequest();
        ProductionLineInfo peer = new ProductionLineInfo(2, "127.0.0.1", 50052);
        coordinator.addPeerIfActive(peer);
        CalibrationRequestThread request = coordinator.requestFromNewPeer(peer);

        coordinator.removePeerAndCleanup(2);
        coordinator.startAdditionalCalibrationRequest(request);

        assertEquals(Thread.State.NEW, request.getState());
        assertEquals(0, coordinator.getMissingPermissionCount());
        assertEquals(null, coordinator.requestFromNewPeer(peer));
        assertTrue(coordinator.waitForCalibrationAccess());
    }

    @Test
    void connectionFailureDoesNotReplaceAMissingPermission() throws Exception {
        Server server = ServerBuilder.forPort(0).build().start();
        int port = server.getPort();
        server.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
        PeerList peers = new PeerList(1);
        ProductionLineInfo peer = new ProductionLineInfo(2, "127.0.0.1", port);
        peers.addPeer(peer);
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, peers);
        coordinator.detectCriticalCondition(96);
        coordinator.beginCalibrationRequest();

        new CalibrationRequestThread(1, coordinator.getCurrentRequestNumber(), 0.2, peer, coordinator).run();

        assertTrue(peers.containsPeer(2));
        assertEquals(1, coordinator.getMissingPermissionCount());
        // Controlled leave is the evidence that makes entry safe.
        coordinator.removePeerAndCleanup(2);
        assertTrue(coordinator.waitForCalibrationAccess());
    }

    @Test
    void connectionFailureDoesNotDiscardAnOutstandingGrant() throws Exception {
        Server server = ServerBuilder.forPort(0).build().start();
        int port = server.getPort();
        server.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
        PeerList peers = new PeerList(1);
        ProductionLineInfo peer = new ProductionLineInfo(2, "127.0.0.1", port);
        peers.addPeer(peer);
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, peers);
        coordinator.handleCalibrationRequest(2, 1, 0.2);
        coordinator.detectCriticalCondition(96);

        ArrayList<CalibrationRequestInfo> grants = new ArrayList<>();
        grants.add(new CalibrationRequestInfo(2, 1));
        CalibrationRequestThread.checkStatus(peer, coordinator, grants);

        assertTrue(peers.containsPeer(2));
        assertEquals(1, coordinator.getGrantedRequestCount());
        assertFalse(coordinator.isRequestStarted());
        coordinator.removePeerAndCleanup(2);
        assertEquals(0, coordinator.getGrantedRequestCount());
        assertTrue(coordinator.beginCalibrationRequest().isEmpty());
        assertTrue(coordinator.waitForCalibrationAccess());
    }

    @Test
    void statusCheckRecoversALostReleaseAndALostGrant() throws Exception {
        PeerList remotePeers = new PeerList(2);
        remotePeers.addPeer(new ProductionLineInfo(1, "127.0.0.1", 50051));
        CalibrationCoordinator remote = new CalibrationCoordinator(2, remotePeers);
        Server server = ServerBuilder.forPort(0)
                .addService(new ProductionLineServiceImpl(remotePeers, remote)).build().start();
        try {
            PeerList peers = new PeerList(1);
            ProductionLineInfo peer = new ProductionLineInfo(2, "127.0.0.1", server.getPort());
            peers.addPeer(peer);
            CalibrationCoordinator local = new CalibrationCoordinator(1, peers);
            local.handleCalibrationRequest(2, 7, 0.1);
            ArrayList<CalibrationRequestInfo> grants = new ArrayList<>();
            grants.add(new CalibrationRequestInfo(2, 7));
            local.detectCriticalCondition(96);

            CalibrationRequestThread.checkStatus(peer, local, grants);
            assertEquals(0, local.getGrantedRequestCount());
            local.beginCalibrationRequest();
            remote.handleCalibrationRequest(1, local.getCurrentRequestNumber(), 0.2);

            CalibrationRequestThread.checkStatus(peer, local, new ArrayList<>());
            assertEquals(0, local.getMissingPermissionCount());
            assertTrue(local.waitForCalibrationAccess());
        } finally {
            server.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    @Test
    void aDeadlineDoesNotRemoveALivePeerOrItsPermission() throws Exception {
        Server server = ServerBuilder.forPort(0)
                .addService(new ProductionLineServiceGrpc.ProductionLineServiceImplBase() {
                    @Override
                    public void getCalibrationStatus(ProductionLineServiceOuterClass.CalibrationStatusRequest request,
                            StreamObserver<ProductionLineServiceOuterClass.CalibrationStatusResponse> observer) {
                        observer.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
                    }
                }).build().start();
        try {
            PeerList peers = new PeerList(1);
            ProductionLineInfo peer = new ProductionLineInfo(2, "127.0.0.1", server.getPort());
            peers.addPeer(peer);
            CalibrationCoordinator coordinator = new CalibrationCoordinator(1, peers);
            coordinator.handleCalibrationRequest(2, 7, 0.2);
            ArrayList<CalibrationRequestInfo> grants = new ArrayList<>();
            grants.add(new CalibrationRequestInfo(2, 7));

            CalibrationRequestThread.checkStatus(peer, coordinator, grants);

            assertTrue(peers.containsPeer(2));
            assertEquals(1, coordinator.getGrantedRequestCount());
        } finally {
            server.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    @Test
    void statusReplyDoesNotClearAGrantCreatedWhileTheRpcWasInFlight() throws Exception {
        PeerList peers = new PeerList(1);
        CalibrationCoordinator coordinator = new CalibrationCoordinator(1, peers);
        Server server = ServerBuilder.forPort(0)
                .addService(new ProductionLineServiceGrpc.ProductionLineServiceImplBase() {
                    @Override
                    public void getCalibrationStatus(ProductionLineServiceOuterClass.CalibrationStatusRequest request,
                            StreamObserver<ProductionLineServiceOuterClass.CalibrationStatusResponse> observer) {
                        coordinator.handleCalibrationRequest(2, 8, 0.2);
                        observer.onNext(ProductionLineServiceOuterClass.CalibrationStatusResponse.newBuilder().build());
                        observer.onCompleted();
                    }
                }).build().start();
        try {
            ProductionLineInfo peer = new ProductionLineInfo(2, "127.0.0.1", server.getPort());
            peers.addPeer(peer);
            coordinator.handleCalibrationRequest(2, 7, 0.2);
            ArrayList<CalibrationRequestInfo> grants = new ArrayList<>();
            grants.add(new CalibrationRequestInfo(2, 7));

            CalibrationRequestThread.checkStatus(peer, coordinator, grants);

            assertEquals(1, coordinator.getGrantedRequestCount());
            coordinator.releaseReceived(2, 8);
            assertEquals(0, coordinator.getGrantedRequestCount());
        } finally {
            server.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    private CalibrationCoordinator coordinator(int id) {
        return new CalibrationCoordinator(id, new PeerList(id));
    }

    private static class BlockingAdditionalRequestThread extends CalibrationRequestThread {
        private final CalibrationCoordinator coordinator;
        private final CountDownLatch started;
        private final CountDownLatch release;

        private BlockingAdditionalRequestThread(CalibrationCoordinator coordinator,
                CountDownLatch started,
                CountDownLatch release) {
            super(
                    coordinator.getOwnId(),
                    1,
                    0.2,
                    new ProductionLineInfo(2, "127.0.0.1", 50052),
                    coordinator,
                    true);
            this.coordinator = coordinator;
            this.started = started;
            this.release = release;
        }

        @Override
        public void run() {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                coordinator.additionalCalibrationRequestFinished();
            }
        }
    }
}
