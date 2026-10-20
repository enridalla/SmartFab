package dps.administrationclient;

import dps.model.ProductionLineInfo;
import dps.model.ProductionLineState;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Scanner;

// Provides a console menu for querying production lines through the administration REST API.
public class AdministrationClient {
    private static final String DEFAULT_ADDRESS = "localhost";
    private static final int DEFAULT_PORT = 8080;
    private static final String PRODUCTION_LINES_PATH = "/production-lines";

    private final String serverAddress;
    private final RestTemplate restTemplate;
    private final Scanner scanner = new Scanner(System.in);

    private AdministrationClient(String address, int port) {
        serverAddress = "http://" + address + ":" + port;
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        // Bound network waits so an unavailable server does not block the menu indefinitely.
        requestFactory.setConnectTimeout(3000);
        requestFactory.setReadTimeout(5000);
        restTemplate = new RestTemplate(requestFactory);
    }

    public static void main(String[] args) {
        String address = DEFAULT_ADDRESS;
        int port = DEFAULT_PORT;

        if (args.length != 0 && args.length != 2) {
            printUsage();
            return;
        }

        if (args.length == 2) {
            address = args[0];

            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("Administration Server port must be an integer");
                return;
            }

            if (address.isBlank() || port <= 0 || port > 65535) {
                System.out.println("Address must not be empty and port must be between 1 and 65535");
                return;
            }
        }

        new AdministrationClient(address, port).run();
    }

    private void run() {
        boolean running = true;

        while (running) {
            printMenu();

            if (!scanner.hasNextLine()) {
                return;
            }

            String choice = scanner.nextLine().trim();
            switch (choice) {
                case "1" -> listProductionLines();
                case "2" -> showProductionLineStates();
                case "3" -> showAverageVibration();
                case "4" -> running = false;
                default -> System.out.println("Invalid choice");
            }

            System.out.println();
        }
    }

    private void listProductionLines() {
        try {
            ResponseEntity<ProductionLineInfo[]> response = restTemplate.getForEntity(
                    serverAddress + PRODUCTION_LINES_PATH,
                    ProductionLineInfo[].class);

            ProductionLineInfo[] productionLines = response.getBody();
            if (productionLines == null || productionLines.length == 0) {
                System.out.println("No active production lines");
                return;
            }

            System.out.println("Active production lines:");
            for (ProductionLineInfo productionLine : productionLines) {
                System.out.println("Line " + productionLine.getId()
                        + " - " + productionLine.getAddress()
                        + ":" + productionLine.getPort());
            }
        } catch (RestClientException e) {
            printRestError(e);
        }
    }

    private void showProductionLineStates() {
        try {
            ResponseEntity<ProductionLineState[]> response = restTemplate.getForEntity(
                    serverAddress + PRODUCTION_LINES_PATH + "/states",
                    ProductionLineState[].class);

            ProductionLineState[] states = response.getBody();
            if (states == null || states.length == 0) {
                System.out.println("No active production lines");
                return;
            }

            System.out.println("Production line states:");
            for (ProductionLineState state : states) {
                System.out.println("Line " + state.getProductionLineId()
                        + ": " + state.getOperationalState());
            }
        } catch (RestClientException e) {
            printRestError(e);
        }
    }

    private void showAverageVibration() {
        System.out.print("Production Line ID: ");
        String idText = readLine();
        System.out.print("t1: ");
        String t1Text = readLine();
        System.out.print("t2: ");
        String t2Text = readLine();

        if (idText == null || t1Text == null || t2Text == null) {
            System.out.println("Input interrupted");
            return;
        }

        int productionLineId;
        long t1;
        long t2;

        try {
            productionLineId = Integer.parseInt(idText.trim());
            t1 = Long.parseLong(t1Text.trim());
            t2 = Long.parseLong(t2Text.trim());
        } catch (NumberFormatException e) {
            System.out.println("ID must be an integer and timestamps must be integer milliseconds");
            return;
        }

        if (t1 > t2) {
            System.out.println("Invalid interval: t1 must be less than or equal to t2");
            return;
        }

        String url = serverAddress + PRODUCTION_LINES_PATH + "/" + productionLineId
                + "/average?t1=" + t1 + "&t2=" + t2;

        try {
            ResponseEntity<Double> response = restTemplate.getForEntity(url, Double.class);
            Double average = response.getBody();

            if (average == null) {
                System.out.println("No telemetry data in the selected interval");
            } else {
                System.out.println("Average vibration: " + average);
            }
        } catch (HttpStatusCodeException e) {
            if (e.getStatusCode().value() == 404) {
                System.out.println("Production Line " + productionLineId + " is not active");
            } else if (e.getStatusCode().value() == 400) {
                System.out.println("Invalid time interval");
            } else {
                System.out.println("Administration Server returned " + e.getStatusCode());
            }
        } catch (RestClientException e) {
            printRestError(e);
        }
    }

    private String readLine() {
        if (!scanner.hasNextLine()) {
            return null;
        }

        return scanner.nextLine();
    }

    private void printRestError(RestClientException exception) {
        if (exception instanceof ResourceAccessException) {
            System.out.println("Administration Server is not reachable");
        } else if (exception instanceof HttpStatusCodeException httpException) {
            System.out.println("Administration Server returned " + httpException.getStatusCode());
        } else {
            System.out.println("REST request failed: " + exception.getMessage());
        }
    }

    private void printMenu() {
        System.out.println("===== SMARTFAB ADMINISTRATION =====");
        System.out.println("1. List production lines");
        System.out.println("2. Show production line states");
        System.out.println("3. Average vibration");
        System.out.println("4. Exit");
        System.out.print("Choice: ");
    }

    private static void printUsage() {
        System.out.println("Usage: AdministrationClient [<Administration Server address> <port>]");
    }
}
