# Use the Ballerina base image
FROM ballerina/ballerina:2201.3.2

# Copy the Ballerina source files
COPY ./ /home/ballerina/src

# Set working directory
WORKDIR /home/ballerina/src

# Build the Ballerina service
RUN bal build ./international_delivery_service.bal

# Set the entry point to run the Ballerina service
CMD ["bal", "run", "international_delivery_service.bal"]

# Expose port 9090 for the HTTP service
EXPOSE 9090
