# Use a minimal base image for Go binaries
FROM alpine:latest

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy the Go binary into the container
COPY worker /app/worker

# Make the binary executable
RUN chmod +x /app/worker

# Command to run the binary
CMD ["./worker"]
