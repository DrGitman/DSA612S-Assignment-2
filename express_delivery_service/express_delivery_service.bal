import ballerinax/kafka;
import ballerina/io;
import ballerinax/mysql;

// Initialize MySQL client with Docker service name
mysql:Client dbClient = check new mysql:Client(user = "root", password = "password", 
    database = "logistics_db", host = "mysql", port = 3306);

// Initialize Kafka listener for express delivery
listener kafka:Listener expressConsumer = check new(kafka:DEFAULT_URL, {
    groupId: "express-delivery-group",  
    topics: "express-delivery"
});

// Initialize Kafka producer for confirmations
kafka:Producer confirmationProducer = check new(kafka:DEFAULT_URL);

// Define record types for customer details and shipment
type CustomerDetails record {
    string firstName;
    string lastName;
    string contactNumber;
};

type Shipment record {
    string deliveryLocation;
    string contactNumber;
    string trackingNumber = "";
    string shipmentType;
    string pickupLocation;
    string preferredTimeSlot;
    string firstName;
    string lastName;
};

type Confirmation record {
    string confirmationId;
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string estimatedDeliveryTime;
    string status;
};

// Service to handle incoming Kafka messages
service on expressConsumer {
    remote function onConsumerRecord(Shipment[] request) returns error? {
        foreach Shipment shipment_details in request {
            // Create a confirmation record
            Confirmation confirmation = {
                confirmationId: shipment_details.trackingNumber,
                shipmentType: shipment_details.shipmentType,
                pickupLocation: shipment_details.pickupLocation,
                deliveryLocation: shipment_details.deliveryLocation,
                estimatedDeliveryTime: "1 day",
                status: "Confirmed"
            };
            io:println(confirmation.confirmationId);

            // Send confirmation message to Kafka
            check confirmationProducer->send({topic: "confirmationShipment", value: confirmation});

            // Print shipment details
            io:println(shipment_details.firstName + " " + shipment_details.lastName + " " + shipment_details.contactNumber + " " + shipment_details.trackingNumber);
        }
    }
}
