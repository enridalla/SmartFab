package dps.productionline.calibration;

import dps.model.OperationalState;
import dps.model.ProductionLineInfo;
import dps.productionline.mqtt.ProductionLineMqttClient;
import dps.productionline.telemetry.MeasurementBuffer;
import dps.productionline.telemetry.SlidingWindow;
import sensor.MonitoringSensor;

import java.util.ArrayList;
import java.util.Random;

// Runs one calibration round, from acquiring peer permissions to releasing them.
public class CalibrationThread extends Thread {
    private final int ownId;
    private final CalibrationCoordinator coordinator;
    private final MonitoringSensor sensor;
    private final MeasurementBuffer buffer;
    private final SlidingWindow slidingWindow;
    private final ProductionLineMqttClient mqttClient;
    private final Random random = new Random();

    public CalibrationThread(int ownId, CalibrationCoordinator coordinator,
            MonitoringSensor sensor, MeasurementBuffer buffer, SlidingWindow slidingWindow,
            ProductionLineMqttClient mqttClient) {
        this.ownId = ownId;
        this.coordinator = coordinator;
        this.sensor = sensor;
        this.buffer = buffer;
        this.slidingWindow = slidingWindow;
        this.mqttClient = mqttClient;
    }

    @Override
    public void run() {
        boolean interrupted = false;
        try {
            ArrayList<ProductionLineInfo> peers;

            while (true) {
                try {
                    peers = coordinator.beginCalibrationRequest();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }

            long requestNumber;
            double criticality;
            synchronized (coordinator) {
                if (coordinator.isExitRequested()
                        || coordinator.getState() != OperationalState.WaitingForCalibration) {
                    return;
                }

                requestNumber = coordinator.getCurrentRequestNumber();
                criticality = coordinator.getCriticality();
            }
            interrupted = sendRequests(peers, requestNumber, criticality) || interrupted;

            boolean enteredCalibration;
            while (true) {
                try {
                    enteredCalibration = coordinator.waitForCalibrationAccess();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }

            if (!enteredCalibration) {
                return;
            }

            mqttClient.publishStateUpdate(
                    OperationalState.UnderCalibration,
                    coordinator.getStateTimestamp(),
                    criticality);

            int duration = random.nextInt(5) + 3;
            System.out.println("Line " + ownId + " calibration duration: " + duration + " seconds");
            interrupted = sleepForDuration(duration) || interrupted;

            buffer.clear();
            slidingWindow.clear();

            ArrayList<CalibrationRequestInfo> deferredRequests;
            synchronized (coordinator) {
                if (!coordinator.isExitRequested()) {
                    sensor.startMeasuring();
                    System.out.println("Line " + ownId + " sensor resumed");
                }
                deferredRequests = coordinator.finishCalibration();
            }
            mqttClient.publishStateUpdate(
                    OperationalState.FullyOperational,
                    coordinator.getStateTimestamp(),
                    null);

            interrupted = sendCompletionMessages(
                    coordinator.getPeers(),
                    deferredRequests,
                    requestNumber) || interrupted;
        } finally {
            coordinator.calibrationWorkerFinished();
            restoreInterrupt(interrupted);
        }
    }

    private boolean sleepForDuration(int durationSeconds) {
        long endTime = System.currentTimeMillis() + durationSeconds * 1000L;
        boolean interrupted = false;

        // Recompute the remaining time after interruptions so calibration still lasts
        // for the chosen duration.
        while (true) {
            long remaining = endTime - System.currentTimeMillis();
            if (remaining <= 0) {
                return interrupted;
            }

            try {
                Thread.sleep(remaining);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
    }

    private boolean sendRequests(ArrayList<ProductionLineInfo> peers,
            long requestNumber, double criticality) {
        ArrayList<Thread> threads = new ArrayList<>();

        // Each peer receives the same immutable identity for this calibration round.
        for (ProductionLineInfo peer : peers) {
            threads.add(new CalibrationRequestThread(
                    ownId,
                    requestNumber,
                    criticality,
                    peer,
                    coordinator));
        }

        if (!coordinator.startCalibrationRequestThreads(threads, requestNumber)) {
            return false;
        }

        return joinThreads(threads);
    }

    private boolean sendCompletionMessages(ArrayList<ProductionLineInfo> peers, ArrayList<CalibrationRequestInfo> deferredRequests, long completedRequestNumber) {
        ArrayList<Thread> threads = new ArrayList<>();

        // Deferred peers now receive the permissions that could not be granted while
        // this line was calibrating.
        for (CalibrationRequestInfo request : deferredRequests) {
            ProductionLineInfo peer = findPeer(peers, request.getRequesterId());

            if (peer == null) {
                System.out.println("Cannot send deferred grant to unknown line " + request.getRequesterId());
            } else {
                threads.add(new CalibrationGrantThread(
                        ownId,
                        request.getRequestNumber(),
                        peer));
            }
        }

        // Send the completed request number to every known peer. A peer removes a grant
        // only if it has a matching record for this round.
        for (ProductionLineInfo peer : peers) {
            threads.add(new CalibrationReleaseThread(
                    ownId,
                    completedRequestNumber,
                    peer));
        }

        return startAndJoin(threads);
    }

    private boolean startAndJoin(ArrayList<Thread> threads) {
        // Start all blocking RPC workers first so network calls run concurrently.
        for (Thread thread : threads) {
            thread.start();
        }

        return joinThreads(threads);
    }

    private boolean joinThreads(ArrayList<Thread> threads) {
        // Wait for every attempt even if this worker is interrupted midway.
        boolean interrupted = false;
        for (Thread thread : threads) {
            while (thread.isAlive()) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }

        return interrupted;
    }

    private ProductionLineInfo findPeer(ArrayList<ProductionLineInfo> peers, int peerId) {
        for (ProductionLineInfo peer : peers) {
            if (peer.getId() == peerId) {
                return peer;
            }
        }

        return null;
    }

    private void restoreInterrupt(boolean interrupted) {
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
