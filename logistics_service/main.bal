import ballerinax/kafka;
import ballerina/io;
import ballerinax/mysql;
import ballerina/sql;
import ballerina/http;
import ballerina/random;

// Define Kafka producer configurations
kafka:ProducerConfiguration producerConfig = {
    clientId: "ballerina-producer",  // Client ID
    acks: "all"                      // Acknowledgment setting
};

listener kafka:Listener logisticConsumer = check new(kafka:DEFAULT_URL, {
    groupId: "logistic-delivery-group",
    topics: "confirmationShipment"
});



// Initialize the Kafka producer with 'bootstrapServers' in the constructor
kafka:Producer kafkaProducer = check new ("localhost:9092", producerConfig);

// Define a record type for the customer request
type CustomerRequest record {
    string firstName;
    string lastName;
    string contactNumber;
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string preferredTimeSlot;
    string trackingNumber;
};


type Confirmation record {
    string confirmationId;
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string estimatedDeliveryTime;
    string status;
};

http:Client http_client = check new ("localhost:9090/logistic");



service /logistic on new http:Listener(9090) {
    private final mysql:Client db;
    private final kafka:Producer kafkaProducer;

    function init() returns error? {
        self.kafkaProducer = check new(kafka:DEFAULT_URL);
        
        self.db = check new("localhost", "root", "passsword", "logisticsdb",3306);
    }


      resource function post sending(CustomerRequest recieved ) returns string|error? {
         
        sql:ParameterizedQuery insert_customer = `INSERT INTO customers (first_name, last_name, contact_number) VALUES (${recieved.firstName},${recieved.lastName},${recieved.contactNumber})`;
        sql:ExecutionResult _ = check self.db->execute(insert_customer); 

        int tracking_number =check random:createIntInRange(1000000000, 9999999999);
        sql:ParameterizedQuery insert_shipment = `INSERT INTO Shipments (shipment_type, pickup_location, delivery_location, preferred_time_slot,tracking_number) VALUES (${recieved.shipmentType},${recieved.pickupLocation},${recieved.deliveryLocation},${recieved.preferredTimeSlot}, ${tracking_number})`;
        sql:ExecutionResult _ = check self.db->execute(insert_shipment);

        check self.kafkaProducer->send({topic: recieved.shipmentType, value:recieved});

        
        io:println("request confirmed");
        return string `packageId: ${tracking_number}`;
        
    }
}

    service on logisticConsumer {

    private final mysql:Client db;

    function init() returns error? {
        self.db = check new("localhost", "root", "password", "logistics_db",3306);
    }
    remote function onConsumerRecord(Confirmation[] recieved ) returns error? {
        foreach Confirmation shipment in recieved {
            sql:ParameterizedQuery check_shipments = `UPDATE Shipments SET status = "Confirmed" WHERE tracking_number = ${shipment.confirmationId}`;
            sql:ExecutionResult _ = check self.db->execute(check_shipments);
            io:println("Shipment confirmed");
            
        }
    }
}
