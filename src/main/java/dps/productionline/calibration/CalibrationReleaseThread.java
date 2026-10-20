package dps.productionline.calibration;

import dps.model.ProductionLineInfo;
import dps.productionline.ProductionLineServiceGrpc;
import dps.productionline.ProductionLineServiceOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;

// Notifies a peer that the local calibration request has completed.
public class CalibrationReleaseThread extends Thread {
    private final int ownId;
    private final long requestNumber;
    private final ProductionLineInfo peer;

    public CalibrationReleaseThread(int ownId, long requestNumber, ProductionLineInfo peer) {
        this.ownId = ownId;
        this.requestNumber = requestNumber;
        this.peer = new ProductionLineInfo(peer);
    }

    @Override
    public void run() {
        ManagedChannel channel = null;

        try {
            // Open a channel to a peer that must be notified about the completed request.
            channel = ManagedChannelBuilder
                    .forAddress(peer.getAddress(), peer.getPort())
                    .usePlaintext()
                    .build();

            // Limit the blocking release call if the peer is unavailable.
            ProductionLineServiceGrpc.ProductionLineServiceBlockingStub stub = ProductionLineServiceGrpc
                    .newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS);

            // Identify the completed request so the peer can release the matching permission.
            ProductionLineServiceOuterClass.CalibrationRelease request = ProductionLineServiceOuterClass.CalibrationRelease
                    .newBuilder()
                    .setReleaserId(ownId)
                    .setReleaserRequestNumber(requestNumber)
                    .build();

            // The empty response acknowledges that the peer processed the release.
            stub.releaseCalibration(request);
        } catch (StatusRuntimeException e) {
            // Report gRPC failures, including connection errors and deadline expiration.
            System.out.println("Calibration release to line " + peer.getId()
                    + " failed: " + e.getStatus().getCode());
        } catch (Exception e) {
            // Keep unexpected release failures local to this worker thread.
            System.out.println("Calibration release to line " + peer.getId()
                    + " failed: " + e.getMessage());
        } finally {
            // Each release attempt uses its own channel and closes it here.
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }
}
