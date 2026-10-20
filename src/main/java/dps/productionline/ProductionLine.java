package dps.productionline;

import dps.model.ProductionLineInfo;
import dps.productionline.calibration.CalibrationCoordinator;
import dps.productionline.grpc.IntroductionThread;
import dps.productionline.grpc.LeaveThread;
import dps.productionline.grpc.ProductionLineServiceImpl;
import dps.productionline.mqtt.ProductionLineMqttClient;
import dps.productionline.mqtt.TelemetryPublisherThread;
import dps.productionline.telemetry.MeasurementBuffer;
import dps.productionline.telemetry.SensorDataThread;
import dps.productionline.telemetry.SlidingWindow;
import sensor.MonitoringSensor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

// Starts a production line and coordinates registration, telemetry, calibration, and shutdown.
public class ProductionLine {
    private static final String REGISTRATION_PATH = "/production-lines";

    public static void main(String[] args) {
        if (args.length != 5) {
            printUsage();
            return;
        }

        int id;
        int grpcPort;
        int administrationPort;

        try {
            id = Integer.parseInt(args[0]);
            grpcPort = Integer.parseInt(args[2]);
            administrationPort = Integer.parseInt(args[4]);
        } catch (NumberFormatException e) {
            System.out.println("ID and ports must be integers");
            printUsage();
            return;
        }

        String address = args[1];
        String administrationAddress = args[3];

        if (address.isBlank() || administrationAddress.isBlank()
                || !isValidPort(grpcPort) || !isValidPort(administrationPort)) {
            System.out.println("Addresses must not be empty and ports must be between 1 and 65535");
            return;
        }

        ProductionLineInfo productionLine = new ProductionLineInfo(id, address, grpcPort);
        PeerList peerList = new PeerList(id);
        CalibrationCoordinator calibrationCoordinator = new CalibrationCoordinator(id, peerList);
        Server server = ServerBuilder.forPort(grpcPort)
                .addService(new ProductionLineServiceImpl(peerList, calibrationCoordinator))
                .build();

        // Start gRPC before registering: once this line becomes discoverable, peers can
        // contact it and introductions may trigger calibration callbacks.
        try {
            server.start();
            System.out.println("Production Line " + id + " gRPC server started on port " + grpcPort);
        } catch (IOException e) {
            System.out.println("Cannot start Production Line " + id + " gRPC server: " + e.getMessage());
            return;
        }

        ProductionLineInfo[] existingPeers = register(
                productionLine,
                administrationAddress,
                administrationPort);

        if (existingPeers == null) {
            server.shutdownNow();
            return;
        }

        ArrayList<ProductionLineInfo> peersToContact = peerList.mergeRegistrationSnapshot(existingPeers);

        System.out.println("Production Line " + id + " registered");
        boolean introductionsCompleted = introduceToPeers(productionLine, peersToContact,
                calibrationCoordinator, administrationAddress, administrationPort);

        MeasurementBuffer buffer = new MeasurementBuffer();
        SlidingWindow slidingWindow = new SlidingWindow();
        MonitoringSensor sensor = new MonitoringSensor(buffer);
        ProductionLineMqttClient mqttClient = new ProductionLineMqttClient(id);
        SensorDataThread sensorDataThread = new SensorDataThread(
                id,
                buffer,
                slidingWindow,
                sensor,
                calibrationCoordinator,
                mqttClient);

        if (!introductionsCompleted || !mqttClient.connect()) {
            if (!introductionsCompleted) {
                System.out.println("Cannot start with an incomplete peer view; cancelling registration");
            }
            controlledExit(
                    productionLine,
                    administrationAddress,
                    administrationPort,
                    peerList,
                    calibrationCoordinator,
                    sensor,
                    sensorDataThread,
                    null,
                    mqttClient,
                    server);
            return;
        }

        // State changes can be published as soon as the sensor starts.
        sensorDataThread.start();
        sensor.startMeasuring();

        TelemetryPublisherThread telemetryPublisher = new TelemetryPublisherThread(
                id,
                sensorDataThread,
                calibrationCoordinator,
                mqttClient);
        telemetryPublisher.start();

        System.out.println("Production Line " + id + " ready");
        System.out.println("Known peers: " + peerList.getPeerIds());
        System.out.println("Type exit to leave SmartFab");

        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            String command = scanner.nextLine().trim();

            if (command.equalsIgnoreCase("exit")) {
                break;
            }

            System.out.println("Unknown command. Type exit to leave SmartFab");
        }

        controlledExit(
                productionLine,
                administrationAddress,
                administrationPort,
                peerList,
                calibrationCoordinator,
                sensor,
                sensorDataThread,
                telemetryPublisher,
                mqttClient,
                server);
    }

    private static void controlledExit(ProductionLineInfo productionLine,
            String administrationAddress,
            int administrationPort,
            PeerList peerList,
            CalibrationCoordinator coordinator,
            MonitoringSensor sensor,
            SensorDataThread sensorDataThread,
            TelemetryPublisherThread telemetryPublisher,
            ProductionLineMqttClient mqttClient,
            Server server) {
        int productionLineId = productionLine.getId();
        boolean interrupted = false;
        System.out.println("Production Line " + productionLineId + " is leaving SmartFab");

        coordinator.requestExit();

        sensor.stopMeasuring();
        sensorDataThread.shutdown();
        interrupted = joinThread(sensor) || interrupted;
        interrupted = joinThread(sensorDataThread) || interrupted;

        while (true) {
            try {
                coordinator.waitUntilReadyToExit();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        if (telemetryPublisher != null) {
            telemetryPublisher.shutdown();
            interrupted = joinThread(telemetryPublisher) || interrupted;
        }

        interrupted = notifyLeaveToPeers(productionLineId, peerList.getPeers()) || interrupted;
        unregister(productionLineId, administrationAddress, administrationPort);

        mqttClient.disconnect();
        server.shutdown();

        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            interrupted = true;
            server.shutdownNow();
        }

        System.out.println("Production Line " + productionLineId + " stopped");
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean notifyLeaveToPeers(int productionLineId,
            ArrayList<ProductionLineInfo> peers) {
        ArrayList<Thread> threads = new ArrayList<>();

        for (ProductionLineInfo peer : peers) {
            threads.add(new LeaveThread(productionLineId, peer));
        }

        for (Thread thread : threads) {
            thread.start();
        }

        return joinThreads(threads);
    }

    // Waits for every worker even if this thread is interrupted midway, and reports
    // the interruption so the caller can restore the flag once shutdown is complete.
    private static boolean joinThreads(ArrayList<Thread> threads) {
        boolean interrupted = false;

        for (Thread thread : threads) {
            interrupted = joinThread(thread) || interrupted;
        }

        return interrupted;
    }

    private static boolean joinThread(Thread thread) {
        boolean interrupted = false;

        while (thread.isAlive()) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        return interrupted;
    }

    private static void unregister(int productionLineId,
            String administrationAddress,
            int administrationPort) {
        String url = "http://" + administrationAddress + ":" + administrationPort
                + REGISTRATION_PATH + "/" + productionLineId;

        try {
            restClient().delete(url);
            System.out.println("Production Line " + productionLineId
                    + " removed from Administration Server");
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode().value() == 404) {
                // Another peer may already have reported the same crash.
                return;
            }
            System.out.println("Administration Server rejected removal of line "
                    + productionLineId + ": " + e.getStatusCode());
        } catch (RestClientException e) {
            System.out.println("Cannot remove Production Line " + productionLineId
                    + " from Administration Server: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid Administration Server address: " + e.getMessage());
        }
    }

    private static ProductionLineInfo[] register(ProductionLineInfo productionLine,
            String administrationAddress,
            int administrationPort) {
        String url = "http://" + administrationAddress + ":" + administrationPort + REGISTRATION_PATH;
        RestTemplate client = restClient();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<ProductionLineInfo> request = new HttpEntity<>(productionLine, headers);

        try {
            ResponseEntity<ProductionLineInfo[]> response = client.postForEntity(url, request,
                    ProductionLineInfo[].class);
            ProductionLineInfo[] peers = response.getBody();

            if (peers == null) {
                System.out.println("Invalid response from Administration Server");
                return null;
            }

            return peers;
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode().value() == 409) {
                System.out.println("Registration rejected: ID " + productionLine.getId() + " already exists");
            } else {
                System.out.println("Registration rejected by Administration Server: " + e.getStatusCode());
            }

            return null;
        } catch (RestClientException e) {
            System.out.println("Administration Server is not available: " + e.getMessage());
            return null;
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid Administration Server address: " + e.getMessage());
            return null;
        }
    }

    private static boolean introduceToPeers(ProductionLineInfo productionLine,
            ArrayList<ProductionLineInfo> peers, CalibrationCoordinator coordinator,
            String administrationAddress, int administrationPort) {
        ArrayList<Thread> threads = new ArrayList<>();

        // Each worker performs one blocking gRPC call, so separate threads let all
        // introductions progress concurrently.
        for (ProductionLineInfo peer : peers) {
            threads.add(new IntroductionThread(productionLine, peer, coordinator));
        }

        for (Thread thread : threads) {
            thread.start();
        }

        if (joinThreads(threads)) {
            Thread.currentThread().interrupt();
        }

        ArrayList<Integer> failedPeers = new ArrayList<>();
        for (Thread thread : threads) {
            IntroductionThread introduction = (IntroductionThread) thread;
            if (!introduction.wasSuccessful()) {
                failedPeers.add(introduction.getPeerId());
            }
        }
        if (failedPeers.isEmpty()) {
            return true;
        }

        // A controlled leave may finish after the POST snapshot but before introduction.
        // Only confirmed deregistration, after calibration completes, makes removal safe.
        String url = "http://" + administrationAddress + ":" + administrationPort + REGISTRATION_PATH;
        try {
            ProductionLineInfo[] registered = restClient().getForObject(url, ProductionLineInfo[].class);
            if (registered == null) {
                return false;
            }
            for (int failedId : failedPeers) {
                for (ProductionLineInfo peer : registered) {
                    if (peer.getId() == failedId) {
                        return false;
                    }
                }
                coordinator.removePeerAndCleanup(failedId);
                System.out.println("Line " + failedId + " completed its leave before introduction");
            }
        } catch (RestClientException | IllegalArgumentException e) {
            System.out.println("Cannot verify departed peers with Administration Server: " + e.getMessage());
            return false;
        }
        return true;
    }

    private static boolean isValidPort(int port) {
        return port > 0 && port <= 65535;
    }

    private static RestTemplate restClient() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(3000);
        factory.setReadTimeout(5000);
        return new RestTemplate(factory);
    }

    private static void printUsage() {
        System.out.println("Usage: ProductionLine <id> <address> <gRPC port> "
                + "<Administration Server address> <Administration Server port>");
    }
}
