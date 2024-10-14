# Use Ballerina base image
FROM ballerina/ballerina:slim

# Copy the Ballerina service file
COPY standard_delivery_service.bal /home/ballerina/standard_delivery_service.bal

# Set the working directory
WORKDIR /home/ballerina

# Expose the necessary ports
EXPOSE 8081

# Run the service
CMD ["ballerina", "run", "standard_delivery_service.bal"]
