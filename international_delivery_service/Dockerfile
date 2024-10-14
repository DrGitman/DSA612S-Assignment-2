# Use Ballerina base image
FROM ballerina/ballerina:slim

# Copy the Ballerina service file
COPY international_delivery_service.bal /home/ballerina/international_delivery_service.bal

# Set the working directory
WORKDIR /home/ballerina

# Expose the necessary ports
EXPOSE 8083

# Run the service
CMD ["ballerina", "run", "international_delivery_service.bal"]
