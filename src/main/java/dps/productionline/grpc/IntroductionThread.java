package dps.productionline.grpc;

import dps.model.ProductionLineInfo;
import dps.productionline.ProductionLineServiceGrpc;
import dps.productionline.ProductionLineServiceOuterClass;
import dps.productionline.calibration.CalibrationCoordinator;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.Status;

import java.util.concurrent.TimeUnit;

// Introduces the local production line to a peer discovered during registration.
public class IntroductionThread extends Thread {
    private final ProductionLineInfo productionLine;
    private final ProductionLineInfo peer;
    private final CalibrationCoordinator coordinator;
    private boolean successful;

    public IntroductionThread(ProductionLineInfo productionLine, ProductionLineInfo peer,
            CalibrationCoordinator coordinator) {
        this.productionLine = productionLine;
        this.peer = new ProductionLineInfo(peer);
        this.coordinator = coordinator;
    }

    @Override
    public void run() {
        ManagedChannel channel = null;

        try {
            System.out.println("Introducing line " + productionLine.getId() + " to line " + peer.getId());

            channel = ManagedChannelBuilder
                    .forAddress(peer.getAddress(), peer.getPort())
                    .usePlaintext()
                    .build();

            // The deadline prevents an unreachable peer from blocking this thread indefinitely.
            ProductionLineServiceGrpc.ProductionLineServiceBlockingStub stub = ProductionLineServiceGrpc
                    .newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS);

            ProductionLineServiceOuterClass.IntroductionRequest request = ProductionLineServiceOuterClass.IntroductionRequest
                    .newBuilder()
                    .setId(productionLine.getId())
                    .setAddress(productionLine.getAddress())
                    .setPort(productionLine.getPort())
                    .build();

            stub.introduce(request);
            successful = true;
            System.out.println("Introduction to line " + peer.getId() + " completed");
        } catch (StatusRuntimeException e) {
            // This explicit response guarantees that the leaving peer has completed
            // calibration. A transport error alone provides no such guarantee.
            if (e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
                coordinator.removePeerAndCleanup(peer.getId());
                successful = true;
                System.out.println("Line " + peer.getId() + " is leaving; introduction is unnecessary");
            } else {
                System.out.println("Introduction to line " + peer.getId() + " failed: "
                        + e.getStatus().getCode());
            }
        } catch (Exception e) {
            System.out.println("Introduction to line " + peer.getId() + " failed: " + e.getMessage());
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }

    public boolean wasSuccessful() {
        return successful;
    }

    public int getPeerId() {
        return peer.getId();
    }
}
