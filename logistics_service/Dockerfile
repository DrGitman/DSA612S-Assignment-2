# Use Ballerina base image
FROM ballerina/ballerina:slim

# Copy the Ballerina service file
COPY logistics_service.bal /home/ballerina/logistics_service.bal

# Set the working directory
WORKDIR /home/ballerina

# Expose the necessary ports (for example, if needed for RESTful API)
EXPOSE 8080

# Run the service
CMD ["ballerina", "run", "logistics_service.bal"]
