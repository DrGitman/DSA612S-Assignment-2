
import ballerinax/kafka;
import ballerina/io;
import ballerinax/mysql;

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

listener kafka:Listener internationalConsumer = check new(kafka:DEFAULT_URL, {
    groupId: "international-delivery-group",  
    topics: "international-delivery"
});

mysql:Client dbClient = check new mysql:Client(user = "root", password = "root", 
 database = "logisticsdb", host = "localhost", port = 3306);

 kafka:Producer confirmationProducer = check new(kafka:DEFAULT_URL);

service on internationalConsumer {
 remote function onConsumerRecord(Shipment[] request) returns error? {
        foreach Shipment shipment_details in request {
            Confirmation confirmation = {
                confirmationId: shipment_details.trackingNumber,
                shipmentType: shipment_details.shipmentType,
                pickupLocation: shipment_details.pickupLocation,
                deliveryLocation: shipment_details.deliveryLocation,
                estimatedDeliveryTime: "1 days",
                status: "Confirmed"
            };
            io:println(confirmation.confirmationId);
            check confirmationProducer->send({topic: "confirmationShipment", value: confirmation});

            io:println(shipment_details.firstName + " " + shipment_details.lastName + " " + shipment_details.contactNumber + " " + shipment_details.trackingNumber);
        }
    }
}
