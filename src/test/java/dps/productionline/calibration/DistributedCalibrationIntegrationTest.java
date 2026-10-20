package dps.productionline.calibration;

import dps.model.OperationalState;
import dps.model.ProductionLineInfo;
import dps.productionline.PeerList;
import dps.productionline.ProductionLineServiceGrpc;
import dps.productionline.ProductionLineServiceOuterClass;
import dps.productionline.grpc.ProductionLineServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Exercises membership changes through real loopback gRPC calls and controlled permission delays.
@Timeout(15)
class DistributedCalibrationIntegrationTest {
    private final ArrayList<Node> nodes = new ArrayList<>();
    private final ArrayList<Thread> workers = new ArrayList<>();

    @Test
    void successiveJoinsExtendAnActiveRequestUntilEveryPeerGrants() throws Exception {
        Node first = node(1, false);
        Node second = node(2, true);
        introduce(second, first);

        startRequest(first);
        second.gate.awaitRequest();
        FutureTask<Boolean> access = awaitAccess(first);
        assertWaiting(first, access, 1);

        Node third = node(3, true);
        introduce(third, first, second);
        third.gate.awaitRequest();
        assertWaiting(first, access, 2);

        second.gate.release();
        awaitMissing(first, 1);
        Node fourth = node(4, true);
        introduce(fourth, first, second, third);
        fourth.gate.awaitRequest();
        assertWaiting(first, access, 2);
        assertEquals("[2, 3, 4]", first.peers.getPeerIds());

        third.gate.release();
        awaitMissing(first, 1);
        assertWaiting(first, access, 1);
        fourth.gate.release();

        assertTrue(access.get(3, TimeUnit.SECONDS));
        assertEquals(OperationalState.UnderCalibration, first.coordinator.getState());
        assertEquals(0, first.coordinator.getMissingPermissionCount());
    }

    @Test
    void controlledLeaveRemovesMissingPermissionAndLateReplyIsHarmless() throws Exception {
        Node first = node(1, false);
        Node second = node(2, true);
        introduce(second, first);

        startRequest(first);
        second.gate.awaitRequest();
        FutureTask<Boolean> access = awaitAccess(first);
        assertWaiting(first, access, 1);

        notifyLeave(second, first);

        assertTrue(access.get(3, TimeUnit.SECONDS));
        assertFalse(first.peers.containsPeer(2));
        assertEquals(0, first.coordinator.getMissingPermissionCount());
        second.gate.release();
        joinWorkers();
        assertEquals(OperationalState.UnderCalibration, first.coordinator.getState());
    }

    @Test
    void leavingWhileWaitingCancelsTheRoundAndRemovesTheDeferredRequest() throws Exception {
        Node first = node(1, false);
        Node second = node(2, false);
        enterCalibrationWithoutPeers(second);
        introduce(first, second);

        startRequest(first);
        joinWorkers();
        assertEquals(1, second.coordinator.getDeferredRequestCount());
        FutureTask<Boolean> access = awaitAccess(first);
        assertWaiting(first, access, 1);

        first.coordinator.requestExit();
        assertFalse(access.get(3, TimeUnit.SECONDS));
        notifyLeave(first, second);

        assertEquals(0, first.coordinator.getCurrentRequestNumber());
        assertEquals(0, first.coordinator.getMissingPermissionCount());
        assertEquals(0, second.coordinator.getDeferredRequestCount());
        assertFalse(second.peers.containsPeer(1));
        assertEquals(OperationalState.UnderCalibration, second.coordinator.getState());
    }

    @Test
    void joiningAnExitingCalibratingLineWaitsUntilThatLineFinishes() throws Exception {
        Node second = node(2, false);
        enterCalibrationWithoutPeers(second);
        second.coordinator.requestExit();

        Node first = node(1, false);
        // Registration can still return a line that must finish calibration before leaving.
        introduce(first, second);
        assertTrue(second.peers.containsPeer(1));
        startRequest(first);
        joinWorkers();
        FutureTask<Boolean> access = awaitAccess(first);
        assertWaiting(first, access, 1);
        assertEquals(1, second.coordinator.getDeferredRequestCount());

        second.coordinator.finishCalibration();
        second.coordinator.calibrationWorkerFinished();
        notifyLeave(second, first);

        assertTrue(access.get(3, TimeUnit.SECONDS));
        assertEquals(OperationalState.FullyOperational, second.coordinator.getState());
        assertEquals(OperationalState.UnderCalibration, first.coordinator.getState());
        assertFalse(first.peers.containsPeer(2));
    }

    private Node node(int id, boolean delayRequests) throws IOException {
        Node node = new Node(id, delayRequests);
        nodes.add(node);
        return node;
    }

    private void introduce(Node joining, Node... existing) {
        for (Node peer : existing) {
            joining.peers.addPeer(peer.info);
        }
        for (Node peer : existing) {
            peer.stub.withDeadlineAfter(3, TimeUnit.SECONDS)
                    .introduce(ProductionLineServiceOuterClass.IntroductionRequest.newBuilder()
                    .setId(joining.info.getId()).setAddress(joining.info.getAddress())
                    .setPort(joining.info.getPort()).build());
        }
    }

    private void startRequest(Node node) throws InterruptedException {
        assertTrue(node.coordinator.detectCriticalCondition(120));
        ArrayList<ProductionLineInfo> peers = node.coordinator.beginCalibrationRequest();
        ArrayList<Thread> requests = new ArrayList<>();
        for (ProductionLineInfo peer : peers) {
            requests.add(new CalibrationRequestThread(node.info.getId(),
                    node.coordinator.getCurrentRequestNumber(), node.coordinator.getCriticality(),
                    peer, node.coordinator));
        }
        workers.addAll(requests);
        assertTrue(node.coordinator.startCalibrationRequestThreads(
                requests, node.coordinator.getCurrentRequestNumber()));
    }

    private void enterCalibrationWithoutPeers(Node node) throws InterruptedException {
        startRequest(node);
        assertTrue(node.coordinator.waitForCalibrationAccess());
    }

    private FutureTask<Boolean> awaitAccess(Node node) {
        FutureTask<Boolean> access = new FutureTask<>(node.coordinator::waitForCalibrationAccess);
        Thread worker = new Thread(access, "test-calibration-access-" + node.info.getId());
        workers.add(worker);
        worker.start();
        return access;
    }

    private void assertWaiting(Node node, FutureTask<Boolean> access, int missing) {
        assertEquals(missing, node.coordinator.getMissingPermissionCount());
        assertThrows(TimeoutException.class, () -> access.get(100, TimeUnit.MILLISECONDS));
        assertEquals(OperationalState.WaitingForCalibration, node.coordinator.getState());
    }

    private void awaitMissing(Node node, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        synchronized (node.coordinator) {
            while (node.coordinator.getMissingPermissionCount() != expected) {
                long remaining = deadline - System.nanoTime();
                assertTrue(remaining > 0, "Permission response did not reach the requester");
                TimeUnit.NANOSECONDS.timedWait(node.coordinator, remaining);
            }
        }
    }

    private void notifyLeave(Node leaving, Node recipient) {
        recipient.stub.withDeadlineAfter(3, TimeUnit.SECONDS)
                .leave(ProductionLineServiceOuterClass.LeaveRequest.newBuilder()
                        .setProductionLineId(leaving.info.getId()).build());
    }

    private void joinWorkers() throws InterruptedException {
        for (Thread worker : workers) {
            worker.join(3000);
            assertFalse(worker.isAlive(), "gRPC request worker did not finish");
        }
    }

    @AfterEach
    void closeNetwork() throws InterruptedException {
        // Wake local waiters and release held responses before closing transport resources.
        for (Node node : nodes) {
            node.coordinator.requestExit();
            if (node.gate != null) {
                node.gate.cancel();
            }
            node.channel.shutdownNow();
            node.server.shutdownNow();
        }
        for (Thread worker : workers) {
            worker.join(3000);
            if (worker.isAlive()) {
                worker.interrupt();
                worker.join(3000);
            }
        }
        for (Node node : nodes) {
            node.coordinator.calibrationWorkerFinished();
            node.coordinator.waitUntilReadyToExit();
        }
        for (Node node : nodes) {
            assertTrue(node.channel.awaitTermination(3, TimeUnit.SECONDS));
            assertTrue(node.server.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    // Each node owns a real server on an ephemeral TCP port, with no dependency on Mosquitto.
    private static class Node {
        private final PeerList peers;
        private final CalibrationCoordinator coordinator;
        private final ProductionLineInfo info;
        private final Server server;
        private final ManagedChannel channel;
        private final ProductionLineServiceGrpc.ProductionLineServiceBlockingStub stub;
        private final GatedService gate;

        private Node(int id, boolean delayRequests) throws IOException {
            peers = new PeerList(id);
            coordinator = new CalibrationCoordinator(id, peers);
            gate = delayRequests ? new GatedService(peers, coordinator) : null;
            ProductionLineServiceImpl service = gate != null
                    ? gate : new ProductionLineServiceImpl(peers, coordinator);
            server = ServerBuilder.forPort(0).addService(service).build().start();
            info = new ProductionLineInfo(id, "127.0.0.1", server.getPort());
            channel = ManagedChannelBuilder.forAddress(info.getAddress(), info.getPort())
                    .usePlaintext().build();
            stub = ProductionLineServiceGrpc.newBlockingStub(channel);
        }
    }

    // Delay only delivery of the response; releasing it executes the production grant logic.
    private static class GatedService extends ProductionLineServiceImpl {
        private ProductionLineServiceOuterClass.CalibrationRequest request;
        private StreamObserver<ProductionLineServiceOuterClass.CalibrationResponse> response;

        private GatedService(PeerList peers, CalibrationCoordinator coordinator) {
            super(peers, coordinator);
        }

        @Override
        public synchronized void requestCalibration(
                ProductionLineServiceOuterClass.CalibrationRequest request,
                StreamObserver<ProductionLineServiceOuterClass.CalibrationResponse> observer) {
            this.request = request;
            response = observer;
            notifyAll();
        }

        private synchronized void awaitRequest() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (response == null) {
                long remaining = deadline - System.nanoTime();
                assertTrue(remaining > 0, "The peer did not receive its calibration request");
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
        }

        private synchronized void release() {
            assertTrue(response != null, "No calibration response is waiting");
            super.requestCalibration(request, response);
            response = null;
        }

        private synchronized void cancel() {
            if (response != null) {
                response.onError(Status.CANCELLED.asRuntimeException());
                response = null;
            }
        }
    }
}
