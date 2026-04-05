# ==========================================
# STAGE 1: THE BUILDER
# ==========================================
# Start with a heavy, full Ubuntu image that has all the development tools
FROM ubuntu:22.04 AS builder

# Install the C++ compilers and CMake
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Set our working folder inside the container
WORKDIR /app

# Copy our entire project from your Windows/WSL laptop into the Linux container
COPY . .

# Run our exact CMake build commands inside the container!
RUN rm -rf build/* && \
    cmake -S . -B build && \
    cmake --build build

# ==========================================
# STAGE 2: THE RUNNER (Production Image)
# ==========================================
# Now, start over with a fresh, extremely lightweight Ubuntu image
FROM ubuntu:22.04

WORKDIR /app

# Steal ONLY the finished executables and config file from the Builder stage.
# We leave behind all the heavy compilers, .cpp files, and CMake garbage!
COPY --from=builder /app/build/redis-server .
COPY --from=builder /app/build/redis-router .
COPY --from=builder /app/redis.conf .

# Open port 6379 to the outside world
EXPOSE 6379

# When the container starts, run our server!
CMD ["./redis-server"]