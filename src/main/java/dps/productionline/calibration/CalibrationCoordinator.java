package dps.productionline.calibration;

import dps.model.OperationalState;
import dps.model.ProductionLineInfo;
import dps.productionline.PeerList;

import java.util.ArrayList;

// Coordinates exclusive calibration access through peer permissions and request priorities.
public class CalibrationCoordinator {
    public static final double CRITICAL_THRESHOLD = 80.0;

    private final int ownId;
    private final PeerList peerList;

    // Synchronized methods protect the complete local state of the distributed
    // calibration protocol from sensor, gRPC and calibration worker threads.
    private OperationalState state = OperationalState.FullyOperational;
    private long stateTimestamp = System.currentTimeMillis();
    private double criticality = 0;
    private boolean requestStarted = false;
    // Request numbers distinguish messages from different calibration rounds.
    private long nextRequestNumber = 0;
    private long currentRequestNumber = 0;
    private boolean exitRequested = false;
    private boolean calibrationWorkerActive = false;
    private int additionalRequestWorkers = 0;

    // Track peers whose permission is still missing for the current local request.
    private final ArrayList<Integer> missingPermissions = new ArrayList<>();
    // Keep requests to grant after this line finishes calibrating.
    private final ArrayList<CalibrationRequestInfo> deferredRequests = new ArrayList<>();
    // Track permissions granted to peers until the matching releases arrive.
    private final ArrayList<CalibrationRequestInfo> grantedRequests = new ArrayList<>();

    public CalibrationCoordinator(int ownId, PeerList peerList) {
        this.ownId = ownId;
        this.peerList = peerList;
    }

    public synchronized boolean detectCriticalCondition(double average) {
        // Only a fully operational line can open a new round, and the threshold is
        // intentionally strict.
        if (exitRequested || calibrationWorkerActive
                || state != OperationalState.FullyOperational
                || average <= CRITICAL_THRESHOLD) {
            return false;
        }

        // Initialize a fresh round before the calibration worker broadcasts it.
        criticality = calculateCriticality(average);
        state = OperationalState.WaitingForCalibration;
        stateTimestamp = System.currentTimeMillis();
        requestStarted = false;
        currentRequestNumber = ++nextRequestNumber;
        missingPermissions.clear();
        calibrationWorkerActive = true;

        System.out.println("Line " + ownId + " -> WaitingForCalibration");
        System.out.println("Line " + ownId + " criticality: " + criticality);
        return true;
    }

    public ArrayList<ProductionLineInfo> beginCalibrationRequest() throws InterruptedException {
        // A line cannot compete for calibration while a peer still holds permission
        // previously granted by this line.
        boolean waitingForReleasesReported = false;
        while (true) {
            synchronized (this) {
                if (exitRequested || state != OperationalState.WaitingForCalibration) {
                    return new ArrayList<>();
                }
                if (grantedRequests.isEmpty()) {
                    ArrayList<ProductionLineInfo> peers = peerList.getPeers();
                    missingPermissions.clear();
                    for (ProductionLineInfo peer : peers) {
                        addIfMissing(missingPermissions, peer.getId());
                    }
                    requestStarted = true;
                    System.out.println("Line " + ownId + " requests calibration (request " + currentRequestNumber + ")");
                    System.out.println("Line " + ownId + " waits for permissions from " + missingPermissions);
                    return peers;
                }
                if (!waitingForReleasesReported) {
                    ArrayList<Integer> grantHolders = new ArrayList<>();
                    for (CalibrationRequestInfo grant : grantedRequests) {
                        addIfMissing(grantHolders, grant.getRequesterId());
                    }
                    System.out.println("Line " + ownId + " waits for releases from " + grantHolders
                            + " before broadcasting request " + currentRequestNumber);
                    waitingForReleasesReported = true;
                }
                wait(5000);
            }
            checkBlockingPeers();
        }
    }

    public synchronized CalibrationRequestThread requestFromNewPeer(ProductionLineInfo peer) {
        // Once a request is active, a newly discovered peer must also grant this round.
        if (exitRequested || state != OperationalState.WaitingForCalibration || !requestStarted
                || !peerList.containsPeer(peer.getId()) || missingPermissions.contains(peer.getId())) {
            return null;
        }

        missingPermissions.add(peer.getId());
        System.out.println("Line " + ownId + " adds new peer " + peer.getId()
                + " to request " + currentRequestNumber + "; missing permissions: " + missingPermissions);
        return new CalibrationRequestThread(
                ownId,
                currentRequestNumber,
                criticality,
                peer,
                this,
                true);
    }

    public synchronized boolean addPeerIfActive(ProductionLineInfo peer) {
        // A line finishing calibration still protects the shared resource. Accept
        // newcomers until it finishes so they wait for its permission and receive leave.
        return (!exitRequested || state == OperationalState.UnderCalibration)
                && peerList.addPeer(peer);
    }

    public synchronized void startAdditionalCalibrationRequest(CalibrationRequestThread request) {
        if (exitRequested || !needsPermission(request.getPeerId(), request.getRequestNumber())) {
            return;
        }

        additionalRequestWorkers++;
        try {
            request.start();
        } catch (RuntimeException e) {
            additionalRequestWorkers--;
            notifyAll();
            throw e;
        }
    }

    public synchronized boolean startCalibrationRequestThreads(
            ArrayList<Thread> threads, long requestNumber) {
        if (exitRequested
                || state != OperationalState.WaitingForCalibration
                || !requestStarted
                || currentRequestNumber != requestNumber) {
            return false;
        }

        for (Thread thread : threads) {
            thread.start();
        }
        return true;
    }

    public synchronized boolean handleCalibrationRequest(int requesterId, long requestNumber, double requesterCriticality) {
        boolean grant;

        // A request already granted is granted again. Otherwise the local state and,
        // when both lines are waiting, the protocol priority decide the response.
        if (containsRequest(grantedRequests, requesterId, requestNumber)) {
            grant = true;
        } else if (state == OperationalState.FullyOperational) {
            grant = true;
        } else if (state == OperationalState.UnderCalibration) {
            grant = false;
        } else if (!requestStarted) {
            // Only a request that has actually been broadcast may defer another line.
            // A pre-broadcast request grants to avoid creating a circular wait.
            grant = true;
        } else {
            grant = hasPriority(requesterCriticality, requesterId, criticality, ownId);
        }

        // Remember either the outstanding grant or the request to serve after the
        // current calibration. The pair of ID and request number makes this idempotent,
        // and only a new record is logged: a waiting peer repeats its request while it
        // has no answer yet.
        if (grant) {
            removeRequest(deferredRequests, requesterId, requestNumber);
            if (addRequestIfMissing(grantedRequests, requesterId, requestNumber)) {
                System.out.println("Line " + ownId + " grants line " + requesterId
                        + " request " + requestNumber);
            }
        } else if (addRequestIfMissing(deferredRequests, requesterId, requestNumber)) {
            System.out.println("Line " + ownId + " defers line " + requesterId
                    + " request " + requestNumber);
        }

        return grant;
    }

    public synchronized boolean handleCalibrationRequestFromPeer(
            int requesterId, long requestNumber, double requesterCriticality) {
        if (!peerList.containsPeer(requesterId)) {
            return false;
        }

        return handleCalibrationRequest(requesterId, requestNumber, requesterCriticality);
    }

    public synchronized void permissionReceived(int peerId, long requestNumber) {
        // Ignore duplicate or stale grants. Only the current round can remove a peer
        // from the set that waitForCalibrationAccess() is watching.
        if (state == OperationalState.WaitingForCalibration && requestStarted
                && currentRequestNumber == requestNumber
                && missingPermissions.remove(Integer.valueOf(peerId))) {
            System.out.println("Line " + ownId + " received permission from line " + peerId
                    + " for request " + requestNumber + "; missing permissions: " + missingPermissions);
            notifyAll();
        }
    }

    public synchronized void releaseReceived(int peerId, long requestNumber) {
        // Matching by request number prevents an old release from clearing a newer
        // permission granted to the same peer.
        boolean changed = removeRequest(grantedRequests, peerId, requestNumber);
        removeRequest(deferredRequests, peerId, requestNumber);

        // A removed outstanding grant may let a waiting local request proceed.
        if (changed) {
            System.out.println("Line " + ownId + " received release from line " + peerId
                    + " for request " + requestNumber);
            notifyAll();
        }
    }

    public boolean waitForCalibrationAccess() throws InterruptedException {
        // Access is safe only after the request was broadcast, every peer granted it,
        // and no peer still holds an earlier grant from this line.
        while (true) {
            synchronized (this) {
                if (exitRequested || state != OperationalState.WaitingForCalibration) {
                    return false;
                }
                if (requestStarted && missingPermissions.isEmpty() && grantedRequests.isEmpty()) {
                    System.out.println("Line " + ownId + " received all permissions");
                    state = OperationalState.UnderCalibration;
                    stateTimestamp = System.currentTimeMillis();
                    System.out.println("Line " + ownId + " -> UnderCalibration at " + stateTimestamp);
                    return true;
                }
                wait(5000);
            }
            checkBlockingPeers();
        }
    }

    private void checkBlockingPeers() throws InterruptedException {
        ArrayList<Thread> checks = new ArrayList<>();
        synchronized (this) {
            if (exitRequested || state != OperationalState.WaitingForCalibration) {
                return;
            }
            ArrayList<CalibrationRequestInfo> grants = new ArrayList<>(grantedRequests);
            for (ProductionLineInfo peer : peerList.getPeers()) {
                boolean holdsGrant = grants.stream().anyMatch(request -> request.getRequesterId() == peer.getId());
                if (holdsGrant || missingPermissions.contains(peer.getId())) {
                    checks.add(new Thread(() -> CalibrationRequestThread.checkStatus(peer, this, grants)));
                }
            }
        }
        // RPCs run outside the monitor; each check has its own deadline.
        checks.forEach(Thread::start);
        boolean interrupted = false;
        for (Thread check : checks) {
            while (check.isAlive()) {
                try {
                    check.join();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            throw new InterruptedException();
        }
    }

    synchronized boolean needsPermission(int peerId, long requestNumber) {
        return !exitRequested && state == OperationalState.WaitingForCalibration
                && requestStarted && currentRequestNumber == requestNumber
                && missingPermissions.contains(peerId) && peerList.containsPeer(peerId);
    }

    public synchronized ArrayList<CalibrationRequestInfo> finishCalibration() {
        ArrayList<CalibrationRequestInfo> requestsToGrant = new ArrayList<>();

        if (state != OperationalState.UnderCalibration) {
            return requestsToGrant;
        }

        // Return a snapshot for the caller to send as delayed grants. They become
        // outstanding grants locally until the corresponding releases arrive.
        requestsToGrant.addAll(deferredRequests);
        for (CalibrationRequestInfo request : requestsToGrant) {
            addRequestIfMissing(
                    grantedRequests,
                    request.getRequesterId(),
                    request.getRequestNumber());
        }

        // Reset the local round before waking threads that were waiting on its state.
        state = OperationalState.FullyOperational;
        stateTimestamp = System.currentTimeMillis();
        criticality = 0;
        requestStarted = false;
        currentRequestNumber = 0;
        missingPermissions.clear();
        deferredRequests.clear();
        notifyAll();

        System.out.println("Line " + ownId + " calibration completed at " + System.currentTimeMillis());
        System.out.println("Line " + ownId + " -> FullyOperational");
        return requestsToGrant;
    }

    public synchronized OperationalState getState() {
        return state;
    }

    public synchronized double getCriticality() {
        return criticality;
    }

    public synchronized long getStateTimestamp() {
        return stateTimestamp;
    }

    public synchronized ArrayList<ProductionLineInfo> getPeers() {
        return peerList.getPeers();
    }

    public synchronized void requestExit() {
        if (exitRequested) {
            return;
        }

        exitRequested = true;

        if (state == OperationalState.WaitingForCalibration) {
            requestStarted = false;
            currentRequestNumber = 0;
            criticality = 0;
            missingPermissions.clear();
            System.out.println("Line " + ownId + " cancelled its calibration request");
        }

        notifyAll();
    }

    public synchronized void waitUntilReadyToExit() throws InterruptedException {
        while (calibrationWorkerActive || additionalRequestWorkers > 0) {
            wait();
        }
    }

    public synchronized boolean isExitRequested() {
        return exitRequested;
    }

    public int getOwnId() {
        return ownId;
    }

    public synchronized void calibrationWorkerFinished() {
        calibrationWorkerActive = false;
        notifyAll();
    }

    synchronized void additionalCalibrationRequestFinished() {
        if (additionalRequestWorkers > 0) {
            additionalRequestWorkers--;
        }
        notifyAll();
    }

    public synchronized void peerLeft(int peerId) {
        boolean changed = missingPermissions.remove(Integer.valueOf(peerId));
        changed = removeRequestsFromPeer(deferredRequests, peerId) || changed;
        changed = removeRequestsFromPeer(grantedRequests, peerId) || changed;

        if (changed) {
            System.out.println("Line " + ownId + " cleared calibration records for departing line "
                    + peerId + "; missing permissions: " + missingPermissions);
            notifyAll();
        }
    }

    public synchronized boolean removePeerAndCleanup(int peerId) {
        boolean removed = peerList.removePeer(peerId);
        peerLeft(peerId);
        return removed;
    }

    synchronized int getMissingPermissionCount() {
        return missingPermissions.size();
    }

    synchronized int getDeferredRequestCount() {
        return deferredRequests.size();
    }

    synchronized int getGrantedRequestCount() {
        return grantedRequests.size();
    }

    synchronized boolean isRequestStarted() {
        return requestStarted;
    }

    public synchronized long getCurrentRequestNumber() {
        return currentRequestNumber;
    }

    public static double calculateCriticality(double average) {
        return (average - CRITICAL_THRESHOLD) / CRITICAL_THRESHOLD;
    }

    public static boolean hasPriority(double firstCriticality, int firstId,
            double secondCriticality, int secondId) {
        int comparison = Double.compare(firstCriticality, secondCriticality);

        // Higher criticality wins; IDs provide a deterministic tie-breaker.
        if (comparison != 0) {
            return comparison > 0;
        }

        return firstId > secondId;
    }

    private void addIfMissing(ArrayList<Integer> values, int value) {
        if (!values.contains(value)) {
            values.add(value);
        }
    }

    private boolean addRequestIfMissing(ArrayList<CalibrationRequestInfo> requests, int requesterId, long requestNumber) {
        if (containsRequest(requests, requesterId, requestNumber)) {
            return false;
        }

        requests.add(new CalibrationRequestInfo(requesterId, requestNumber));
        return true;
    }

    private boolean containsRequest(ArrayList<CalibrationRequestInfo> requests, int requesterId, long requestNumber) {
        for (CalibrationRequestInfo request : requests) {
            if (request.getRequesterId() == requesterId && request.getRequestNumber() == requestNumber) {
                return true;
            }
        }

        return false;
    }

    private boolean removeRequest(ArrayList<CalibrationRequestInfo> requests, int requesterId, long requestNumber) {
        for (int i = 0; i < requests.size(); i++) {
            CalibrationRequestInfo request = requests.get(i);
            if (request.getRequesterId() == requesterId && request.getRequestNumber() == requestNumber) {
                requests.remove(i);
                return true;
            }
        }

        return false;
    }

    private boolean removeRequestsFromPeer(ArrayList<CalibrationRequestInfo> requests, int requesterId) {
        boolean changed = false;

        for (int i = requests.size() - 1; i >= 0; i--) {
            if (requests.get(i).getRequesterId() == requesterId) {
                requests.remove(i);
                changed = true;
            }
        }

        return changed;
    }
}
