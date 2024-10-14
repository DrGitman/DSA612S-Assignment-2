import ballerina/kafka;
import ballerinax/mongodb;

listener kafka:Listener standardDeliveryListener = new({
    bootstrapServers: "localhost:9092",
    groupId: "standard-delivery",
    topics: ["standard-delivery-requests"]
});

// Initialize MongoDB client
mongodb:Client dbClient = check new("mongodb://localhost:27017", "logistics");

service on standardDeliveryListener {
    remote function onConsumerRecord(kafka:ConsumerRecord[] records) returns error? {
        foreach var record in records {
            string request = check string:fromBytes(record.value);
            json requestJson = check json:fromString(request);
            
            // Save shipment and delivery schedule to MongoDB
            check saveShipmentDetails(requestJson.shipment);
            check saveDeliverySchedule({
                "pickupTime": "2024-10-15T10:00:00Z",
                "deliveryTime": "2024-10-16T12:00:00Z"
            });
        }
    }
}

function saveShipmentDetails(json shipmentData) returns error? {
    check dbClient->insert("shipments", shipmentData);
}

function saveDeliverySchedule(json scheduleData) returns error? {
    check dbClient->insert("delivery_schedules", scheduleData);
}
