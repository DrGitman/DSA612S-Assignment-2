import ballerina/http;
import ballerinax/kafka;
import ballerina/io;
import ballerinax/mysql;
import ballerina/sql;
import ballerina/random;

// Define Kafka producer configurations
kafka:ProducerConfiguration producerConfig = {
    clientId: "ballerina-producer",  // Client ID
    acks: "all"                      // Acknowledgment setting
};

// Initialize the Kafka listener
listener kafka:Listener logisticConsumer = check new(kafka:DEFAULT_URL, {
    groupId: "logistic-delivery-group",
    topics: "confirmationShipment"
});

// Initialize the Kafka producer with Docker service name
kafka:Producer kafkaProducer = check new ("kafka:9092", producerConfig);

// Define a record type for the customer request
type CustomerRequest record {
    string firstName;
    string lastName;
    string contactNumber;
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string preferredTimeSlot;
};

// Define a record type for shipment confirmation
type Confirmation record {
    string confirmationId;
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string estimatedDeliveryTime;
    string status;
};

// Initialize the HTTP client with Docker service name
http:Client http_client = check new ("http://logistics_service:9090/logistic");

// Define the main HTTP service
service /logistic on new http:Listener(9090) {
    private final mysql:Client db;
    private final kafka:Producer kafkaProducer;

    // Constructor to initialize MySQL and Kafka
    function init() returns error? {
        self.kafkaProducer = check new(kafka:DEFAULT_URL);
        self.db = check new("mysql", "root", "password", "logistics_db", 3306);
    }

    // Endpoint to handle sending requests
    resource function post sending(CustomerRequest received) returns string|error? {
        // Insert customer details into the database
        sql:ParameterizedQuery insert_customer = `INSERT INTO customers (first_name, last_name, contact_number) VALUES (${received.firstName}, ${received.lastName}, ${received.contactNumber})`;
        sql:ExecutionResult _ = check self.db->execute(insert_customer);

        // Generate a tracking number
        int tracking_number = check random:createIntInRange(1000000000, 9999999999);
        sql:ParameterizedQuery insert_shipment = `INSERT INTO shipments (shipment_type, pickup_location, delivery_location, preferred_time_slot, tracking_number) VALUES (${received.shipmentType}, ${received.pickupLocation}, ${received.deliveryLocation}, ${received.preferredTimeSlot}, ${tracking_number})`;
        sql:ExecutionResult _ = check self.db->execute(insert_shipment);

        // Send message to Kafka
        check self.kafkaProducer->send({topic: received.shipmentType, value: received});

        io:println("Request confirmed");
        return string `Package ID: ${tracking_number}`;
    }
}

// Kafka consumer service for confirmation
service on logisticConsumer {
    private final mysql:Client db;

    function init() returns error? {
        self.db = check new("mysql", "root", "password", "logistics_db", 3306);
    }

    remote function onConsumerRecord(Confirmation[] received) returns error? {
        foreach Confirmation shipment in received {
            sql:ParameterizedQuery check_shipments = `UPDATE shipments SET status = "Confirmed" WHERE tracking_number = ${shipment.confirmationId}`;
            sql:ExecutionResult _ = check self.db->execute(check_shipments);
            io:println("Shipment confirmed");
        }
    }
}
