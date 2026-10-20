package dps.productionline.calibration;

import dps.model.ProductionLineInfo;
import dps.productionline.ProductionLineServiceGrpc;
import dps.productionline.ProductionLineServiceOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;

// Delivers a deferred calibration permission to the peer that requested it.
public class CalibrationGrantThread extends Thread {
    private final int ownId;
    private final long requesterRequestNumber;
    private final ProductionLineInfo peer;

    public CalibrationGrantThread(int ownId, long requesterRequestNumber,
            ProductionLineInfo peer) {
        this.ownId = ownId;
        this.requesterRequestNumber = requesterRequestNumber;
        this.peer = new ProductionLineInfo(peer);
    }

    @Override
    public void run() {
        ManagedChannel channel = null;

        try {
            // Open a channel to the line whose request was previously deferred.
            channel = ManagedChannelBuilder
                    .forAddress(peer.getAddress(), peer.getPort())
                    .usePlaintext()
                    .build();

            // Limit the blocking grant call in case the requester is no longer reachable.
            ProductionLineServiceGrpc.ProductionLineServiceBlockingStub stub = ProductionLineServiceGrpc
                    .newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS);

            // Name the granter and the original request so stale grants can be ignored.
            ProductionLineServiceOuterClass.CalibrationGrant grant = ProductionLineServiceOuterClass.CalibrationGrant
                    .newBuilder()
                    .setGranterId(ownId)
                    .setRequesterRequestNumber(requesterRequestNumber)
                    .build();

            // The empty response acknowledges delivery of the deferred permission.
            stub.grantCalibration(grant);
            System.out.println("Line " + ownId + " grants deferred line "
                    + peer.getId() + " request " + requesterRequestNumber);
        } catch (StatusRuntimeException e) {
            // Report transport failures without terminating the production line process.
            System.out.println("Calibration grant to line " + peer.getId()
                    + " failed: " + e.getStatus().getCode());
        } catch (Exception e) {
            // Keep unexpected grant failures local to this worker thread.
            System.out.println("Calibration grant to line " + peer.getId()
                    + " failed: " + e.getMessage());
        } finally {
            // This worker does not reuse its channel after sending the grant.
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }
}
