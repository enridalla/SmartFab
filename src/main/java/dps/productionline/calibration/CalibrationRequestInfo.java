package dps.productionline.calibration;

// Identifies one peer request and its round, so messages from older rounds can be ignored.
final class CalibrationRequestInfo {
    private final int requesterId;
    private final long requestNumber;

    CalibrationRequestInfo(int requesterId, long requestNumber) {
        this.requesterId = requesterId;
        this.requestNumber = requestNumber;
    }

    int getRequesterId() {
        return requesterId;
    }

    long getRequestNumber() {
        return requestNumber;
    }
}
