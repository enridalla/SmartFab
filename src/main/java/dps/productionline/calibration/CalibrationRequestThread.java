package dps.productionline.calibration;

import dps.model.ProductionLineInfo;
import dps.productionline.ProductionLineServiceGrpc;
import dps.productionline.ProductionLineServiceOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

// Requests a peer's calibration permission and reconciles outstanding grants through status checks.
public class CalibrationRequestThread extends Thread {
    private final int ownId;
    private final long requestNumber;
    private final double criticality;
    private final ProductionLineInfo peer;
    private final CalibrationCoordinator coordinator;
    private final boolean trackedForExit;

    public CalibrationRequestThread(int ownId, long requestNumber, double criticality,
            ProductionLineInfo peer,
            CalibrationCoordinator coordinator) {
        this(ownId, requestNumber, criticality, peer, coordinator, false);
    }

    CalibrationRequestThread(int ownId, long requestNumber, double criticality,
            ProductionLineInfo peer,
            CalibrationCoordinator coordinator,
            boolean trackedForExit) {
        this.ownId = ownId;
        this.requestNumber = requestNumber;
        this.criticality = criticality;
        this.peer = new ProductionLineInfo(peer);
        this.coordinator = coordinator;
        this.trackedForExit = trackedForExit;
    }

    @Override
    public void run() {
        ManagedChannel channel = null;

        try {
            // Open a channel to the peer that must authorize this calibration request.
            channel = ManagedChannelBuilder
                    .forAddress(peer.getAddress(), peer.getPort())
                    .usePlaintext()
                    .build();

            // Bound the blocking call so a missing peer cannot hold this worker forever.
            ProductionLineServiceGrpc.ProductionLineServiceBlockingStub stub = ProductionLineServiceGrpc
                    .newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS);

            // Identify the local request and include its criticality for the priority decision.
            ProductionLineServiceOuterClass.CalibrationRequest request = ProductionLineServiceOuterClass.CalibrationRequest
                    .newBuilder()
                    .setRequesterId(ownId)
                    .setRequestNumber(requestNumber)
                    .setCriticality(criticality)
                    .build();

            ProductionLineServiceOuterClass.CalibrationResponse response = stub.requestCalibration(request);

            // Immediate grants count now; a deferred peer will send a separate grant later.
            if (response.getGranted()) {
                coordinator.permissionReceived(peer.getId(), requestNumber);
            }
        } catch (StatusRuntimeException e) {
            // Report gRPC failures, including connection errors and deadline expiration.
            System.out.println("Calibration request to line " + peer.getId()
                    + " failed: " + e.getStatus().getCode());
            // A transport failure does not prove that the peer left calibration.
            // Keep its permission pending; status checks retry the same round.
        } catch (Exception e) {
            // Keep unexpected request failures local to this worker thread.
            System.out.println("Calibration request to line " + peer.getId()
                    + " failed: " + e.getMessage());
        } finally {
            // Each request thread owns and closes its channel.
            if (channel != null) {
                channel.shutdownNow();
            }
            if (trackedForExit) {
                coordinator.additionalCalibrationRequestFinished();
            }
        }
    }

    int getPeerId() {
        return peer.getId();
    }

    long getRequestNumber() {
        return requestNumber;
    }

    static void checkStatus(ProductionLineInfo peer, CalibrationCoordinator coordinator,
            ArrayList<CalibrationRequestInfo> outstandingGrants) {
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forAddress(peer.getAddress(), peer.getPort())
                    .usePlaintext().build();
            ProductionLineServiceGrpc.ProductionLineServiceBlockingStub stub =
                    ProductionLineServiceGrpc.newBlockingStub(channel);
            long activeRequest = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getCalibrationStatus(ProductionLineServiceOuterClass.CalibrationStatusRequest
                            .newBuilder().build()).getCurrentRequestNumber();

            // Only reconcile grants that existed before the RPC started.
            for (CalibrationRequestInfo grant : outstandingGrants) {
                if (grant.getRequesterId() == peer.getId() && grant.getRequestNumber() != activeRequest) {
                    coordinator.releaseReceived(peer.getId(), grant.getRequestNumber());
                }
            }

            long requestNumber;
            double criticality;
            synchronized (coordinator) {
                requestNumber = coordinator.getCurrentRequestNumber();
                if (!coordinator.needsPermission(peer.getId(), requestNumber)) {
                    return;
                }
                criticality = coordinator.getCriticality();
            }
            // Repeating the same round also recovers a grant lost in transit.
            ProductionLineServiceOuterClass.CalibrationResponse response = stub
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .requestCalibration(ProductionLineServiceOuterClass.CalibrationRequest.newBuilder()
                            .setRequesterId(coordinator.getOwnId()).setRequestNumber(requestNumber)
                            .setCriticality(criticality).build());
            if (response.getGranted()) {
                coordinator.permissionReceived(peer.getId(), requestNumber);
            }
        } catch (StatusRuntimeException e) {
            // Only an explicit leave removes membership. Neither a deadline nor a
            // broken connection proves that a peer has finished calibrating.
            System.out.println("Calibration status from line " + peer.getId()
                    + " failed: " + e.getStatus().getCode());
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }
}
