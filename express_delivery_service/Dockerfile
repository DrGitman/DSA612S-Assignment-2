# Use Ballerina base image
FROM ballerina/ballerina:slim

# Copy the Ballerina service file
COPY express_delivery_service.bal /home/ballerina/express_delivery_service.bal

# Set the working directory
WORKDIR /home/ballerina

# Expose the necessary ports
EXPOSE 8082

# Run the service
CMD ["ballerina", "run", "express_delivery_service.bal"]
