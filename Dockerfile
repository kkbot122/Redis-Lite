# ==========================================
# STAGE 1: BUILDER
# ==========================================
FROM ubuntu:22.04 AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    liblua5.3-dev \
    pkg-config \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install latest CMake manually (no systemd issues)
RUN curl -L https://github.com/Kitware/CMake/releases/download/v4.3.1/cmake-4.3.1-linux-x86_64.sh -o cmake.sh && \
    chmod +x cmake.sh && \
    ./cmake.sh --skip-license --prefix=/usr/local && \
    rm cmake.sh

WORKDIR /app
COPY . .

RUN rm -rf build && \
    cmake -S . -B build && \
    cmake --build build

# ==========================================
# STAGE 2: RUNNER
# ==========================================
FROM ubuntu:22.04

WORKDIR /app

COPY --from=builder /app/build/redis-server .
COPY --from=builder /app/build/redis-router .
COPY --from=builder /app/redis.conf .

EXPOSE 6379

CMD ["./redis-server"]