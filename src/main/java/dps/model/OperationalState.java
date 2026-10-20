package dps.model;

// Defines the stages followed by a line as it detects a problem and performs calibration.
public enum OperationalState {
    // The line is measuring and can grant calibration access to peers.
    FullyOperational,
    // A critical value was detected and the line is waiting for distributed permission.
    WaitingForCalibration,
    // The line currently owns calibration access.
    UnderCalibration
}
