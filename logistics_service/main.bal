import ballerina/http;
import ballerina/io;
import ballerinax/kafka;

type Package readonly & record {
    string customer_name;
    string contact_number;
    string pickup_location;
    string delivery_location;
    string delivery_type;
    string preferred_times;
};

type Delivery readonly & record {
    string delivery_type;
    string delivery_time;
    string delivery_day;
};

configurable string groupId = "customers";
configurable string new_delivery_request = "new-delivery-requests";
configurable string delivery_schedule_response = "delivery-schedule-response";
configurable decimal pollingInterval = 2;
configurable string kafkaEndpoint = "172.25.0.11:9092";

// Kafka producer configuration
kafka:ProducerConfig producerConfig = {
    bootstrapServers: "localhost:9092",
    clientId: "logistics-service",
    acks: "all",
    retryCount: 3
};

// Kafka consumer configuration
kafka:ConsumerConfig consumerConfig = {
    bootstrapServers: "localhost:9092",
    groupId: "logistics-group",
    topics: ["logistics-requests"]
};

// HTTP service to receive client requests
service / on new http:Listener(8080) {
    resource function post request(@http:Payload DeliveryRequest payload) returns json|error {
        // Process the delivery request
        json response = check processDeliveryRequest(payload);
        return response;
    }
}

// Kafka consumer service to listen for delivery requests
service on new kafka:Listener(consumerConfig) {
    remote function onConsumerRecord(kafka:ConsumerRecord[] records) {
        foreach var record in records {
            json|error msg = record.value.fromJsonString();
            if (msg is json) {
                DeliveryRequest request = check msg.cloneWithType(DeliveryRequest);
                json|error response = processDeliveryRequest(request);
                if (response is json) {
                    io:println("Processed request: ", response);
                } else {
                    io:println("Error processing request: ", response.message());
                }
            }
        }
    }
}

// Function to process delivery requests
function processDeliveryRequest(DeliveryRequest request) returns json|error {
    // Determine the appropriate delivery service based on the request type
    string topic = getDeliveryTopic(request.shipmentType);

    // Create a Kafka producer
    kafka:Producer producer = check new (producerConfig);

    // Send the request to the appropriate delivery service
    check producer->send({
        topic: topic,
        value: request.toJsonString().toBytes()
    });

    // Close the producer
    check producer-&gt;close();

    // Return a response to the client
    return {
        message: "Request received and forwarded to " + topic,
        trackingId: generateTrackingId()
    };
}

// Function to determine the appropriate Kafka topic based on shipment type
function getDeliveryTopic(string shipmentType) returns string {
    match shipmentType {
        "standard" => { return "standard-delivery"; }
        "express" => { return "express-delivery"; }
        "international" => { return "international-delivery"; }
        _ => { return "standard-delivery"; } // Default to standard delivery
    }
}

// Function to generate a unique tracking ID (simplified version)
function generateTrackingId() returns string {
    return io:uuid();
}

// Record type for delivery requests
type DeliveryRequest record {
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string[] preferredTimeSlots;
    CustomerInfo customerInfo;
};

// Record type for customer information
type CustomerInfo record {
    string firstName;
    string lastName;
    string contactNumber;
};