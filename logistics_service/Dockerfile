# Use the Ballerina base image
FROM ballerina/ballerina:2201.3.2

# Copy the Ballerina source files
COPY ./ /home/ballerina/src

# Set working directory
WORKDIR /home/ballerina/src

# Build the Ballerina service
RUN bal build ./logistics.bal

# Set the entry point to run the Ballerina service
CMD ["bal", "run", "central_logistics.bal"]

# Expose port 9090 for the HTTP service
EXPOSE 9090
