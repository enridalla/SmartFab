package dps.administration;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// Starts the REST administration server and its MQTT monitoring components.
@SpringBootApplication
public class AdministrationServer {
    public static void main(String[] args) {
        SpringApplication.run(AdministrationServer.class, args);
        System.out.println("Administration Server started");
    }
}
