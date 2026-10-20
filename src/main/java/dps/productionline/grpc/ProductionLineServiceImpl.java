package dps.productionline.grpc;

import dps.model.ProductionLineInfo;
import dps.model.OperationalState;
import dps.productionline.PeerList;
import dps.productionline.ProductionLineServiceGrpc;
import dps.productionline.ProductionLineServiceOuterClass;
import dps.productionline.calibration.CalibrationCoordinator;
import dps.productionline.calibration.CalibrationRequestThread;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

// Handles peer membership and calibration messages received through gRPC.
public class ProductionLineServiceImpl extends ProductionLineServiceGrpc.ProductionLineServiceImplBase {
    private final PeerList peerList;
    private final CalibrationCoordinator calibrationCoordinator;

    public ProductionLineServiceImpl(PeerList peerList, CalibrationCoordinator calibrationCoordinator) {
        this.peerList = peerList;
        this.calibrationCoordinator = calibrationCoordinator;
    }

    @Override
    public void introduce(ProductionLineServiceOuterClass.IntroductionRequest request,
            StreamObserver<ProductionLineServiceOuterClass.IntroductionResponse> responseObserver) {
        // Rebuild the peer description from the identity and endpoint in the request.
        ProductionLineInfo peer = new ProductionLineInfo(
                request.getId(),
                request.getAddress(),
                request.getPort());

        CalibrationRequestThread calibrationRequest = null;

        // Only a new peer changes the local view or needs the current calibration request.
        synchronized (calibrationCoordinator) {
            if (calibrationCoordinator.isExitRequested()
                    && calibrationCoordinator.getState() != OperationalState.UnderCalibration) {
                responseObserver.onError(Status.FAILED_PRECONDITION
                        .withDescription("Line is leaving and has no active calibration").asRuntimeException());
                return;
            }
            if (calibrationCoordinator.addPeerIfActive(peer)) {
                System.out.println("New peer: line " + peer.getId());
                System.out.println("Known peers: " + peerList.getPeerIds());
                calibrationRequest = calibrationCoordinator.requestFromNewPeer(peer);
            }
        }

        // Acknowledge the introduction before starting any callback to the new peer.
        ProductionLineServiceOuterClass.IntroductionResponse response = ProductionLineServiceOuterClass.IntroductionResponse
                .newBuilder().build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();

        // Contact the new peer only when a local calibration request is already active.
        if (calibrationRequest != null) {
            calibrationCoordinator.startAdditionalCalibrationRequest(calibrationRequest);
        }
    }

    @Override
    public void requestCalibration(ProductionLineServiceOuterClass.CalibrationRequest request,
            StreamObserver<ProductionLineServiceOuterClass.CalibrationResponse> responseObserver) {
        // The coordinator either grants immediately or records this request for a later grant.
        boolean granted = calibrationCoordinator.handleCalibrationRequestFromPeer(
                request.getRequesterId(),
                request.getRequestNumber(),
                request.getCriticality());

        // Tell the requester whether it can count this peer's permission immediately.
        ProductionLineServiceOuterClass.CalibrationResponse response = ProductionLineServiceOuterClass.CalibrationResponse
                .newBuilder()
                .setGranted(granted)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void grantCalibration(ProductionLineServiceOuterClass.CalibrationGrant request,
            StreamObserver<ProductionLineServiceOuterClass.CalibrationGrantResponse> responseObserver) {
        // Apply the deferred permission to the matching local calibration request.
        calibrationCoordinator.permissionReceived(
                request.getGranterId(),
                request.getRequesterRequestNumber());

        // Return an empty acknowledgement once the coordinator has processed the grant.
        ProductionLineServiceOuterClass.CalibrationGrantResponse response = ProductionLineServiceOuterClass.CalibrationGrantResponse
                .newBuilder().build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void releaseCalibration(ProductionLineServiceOuterClass.CalibrationRelease request,
            StreamObserver<ProductionLineServiceOuterClass.CalibrationReleaseResponse> responseObserver) {
        // Remove the permission held for this peer's completed request.
        calibrationCoordinator.releaseReceived(
                request.getReleaserId(),
                request.getReleaserRequestNumber());

        // Return an empty acknowledgement after the release has been recorded.
        ProductionLineServiceOuterClass.CalibrationReleaseResponse response = ProductionLineServiceOuterClass.CalibrationReleaseResponse
                .newBuilder().build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getCalibrationStatus(ProductionLineServiceOuterClass.CalibrationStatusRequest request,
            StreamObserver<ProductionLineServiceOuterClass.CalibrationStatusResponse> responseObserver) {
        responseObserver.onNext(ProductionLineServiceOuterClass.CalibrationStatusResponse.newBuilder()
                .setCurrentRequestNumber(calibrationCoordinator.getCurrentRequestNumber()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void leave(ProductionLineServiceOuterClass.LeaveRequest request,
            StreamObserver<ProductionLineServiceOuterClass.LeaveResponse> responseObserver) {
        int productionLineId = request.getProductionLineId();

        if (calibrationCoordinator.removePeerAndCleanup(productionLineId)) {
            System.out.println("Line " + productionLineId + " left the network");
            System.out.println("Known peers: " + peerList.getPeerIds());
        }

        ProductionLineServiceOuterClass.LeaveResponse response = ProductionLineServiceOuterClass.LeaveResponse
                .newBuilder().build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
