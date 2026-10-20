package dps.model;

// Carries mutable data exchanged through REST to locate a production line over gRPC.
public class ProductionLineInfo {
    private Integer id;
    private String address;
    private int port;
    private Long registrationTimestamp;

    public ProductionLineInfo() {
        // Spring uses the empty constructor when it creates an object from JSON.
    }

    public ProductionLineInfo(int id, String address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
        this.registrationTimestamp = System.currentTimeMillis();
    }

    public ProductionLineInfo(ProductionLineInfo other) {
        // Registries and peer lists use copies to avoid sharing this mutable object.
        this(other.id, other.address, other.port);
        this.registrationTimestamp = other.registrationTimestamp;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Long getRegistrationTimestamp() {
        return registrationTimestamp;
    }

    public void setRegistrationTimestamp(Long registrationTimestamp) {
        this.registrationTimestamp = registrationTimestamp;
    }
}
