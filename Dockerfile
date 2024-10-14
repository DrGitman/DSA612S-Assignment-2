# Use the official Ballerina image
FROM ballerina/ballerina:latest

# Set the working directory
WORKDIR /usr/src/app

# Copy the Ballerina files to the container
COPY . .

# Build the application
RUN bal build standard.bal

# Command to run the client code
CMD ["bal", "run", "logistics_client.bal"]
