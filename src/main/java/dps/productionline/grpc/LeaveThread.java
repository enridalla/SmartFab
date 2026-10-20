package dps.productionline.grpc;

import dps.model.ProductionLineInfo;
import dps.productionline.ProductionLineServiceGrpc;
import dps.productionline.ProductionLineServiceOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;

// Notifies a peer that the local production line is leaving the network.
public class LeaveThread extends Thread {
    private final int productionLineId;
    private final ProductionLineInfo peer;

    public LeaveThread(int productionLineId, ProductionLineInfo peer) {
        this.productionLineId = productionLineId;
        this.peer = new ProductionLineInfo(peer);
    }

    @Override
    public void run() {
        ManagedChannel channel = null;

        try {
            channel = ManagedChannelBuilder
                    .forAddress(peer.getAddress(), peer.getPort())
                    .usePlaintext()
                    .build();

            // Bound the notification time so an unreachable peer cannot stall shutdown.
            ProductionLineServiceGrpc.ProductionLineServiceBlockingStub stub = ProductionLineServiceGrpc
                    .newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS);

            ProductionLineServiceOuterClass.LeaveRequest request = ProductionLineServiceOuterClass.LeaveRequest
                    .newBuilder()
                    .setProductionLineId(productionLineId)
                    .build();

            stub.leave(request);
        } catch (StatusRuntimeException e) {
            System.out.println("Leave notification to line " + peer.getId()
                    + " failed: " + e.getStatus().getCode());
        } catch (Exception e) {
            System.out.println("Leave notification to line " + peer.getId()
                    + " failed: " + e.getMessage());
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }
}
