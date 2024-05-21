# Use a minimal base image for Go binaries
FROM alpine:latest

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy the Go binary into the container
COPY client /app/client

# Make the binary executable
RUN chmod +x /app/client

# Command to run the binary
ENTRYPOINT ["/app/client"]
