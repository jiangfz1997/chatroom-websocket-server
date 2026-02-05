# chatroom-websocket-server
## WebSocket Server

This module handles real-time messaging via WebSocket connections. It is responsible for:

- Maintaining persistent connections with users
- Receiving and broadcasting messages
- Integrating with Kafka for distributed message delivery
- Writing recent messages to Redis for caching
- Validating login tokens via Redis

---

## Directory Structure

| Folder / File     | Description |
|-------------------|-------------|
| `auth/`           | Token validation logic, using Redis as the session store. |
| `config/`         | Configuration loading (from `.env`, environment variables). |
| `dynamodb/`       | DynamoDB writing logic (optional, for fallback/extension). |
| `kafka/`          | Kafka producer/consumer setup and message routing logic. |
| `logger/`         | Unified logger initialization. |
| `logs/`           | Directory for output log files. |
| `redis/`          | Redis client setup and recent message caching logic. |
| `utils/`          | General-purpose helpers (e.g., ID generation, error formatting). |
| `ws/`             | Core WebSocket server: connection handling, room management, message dispatch. |
| `main.go`         | Program entry point: initializes services and starts the server. |
| `.env`            | Service configuration file. |
| `Dockerfile`      | Docker build definition for deployment. |
| `go.mod`, `go.sum`| Go module dependencies. |

---

## Docker Build & Run

To build and run the WebSocket server independently:

```bash
# Build Docker image
docker build -t websocket-server .

# Run the container with environment configuration
docker run -p 8081:8081 --env-file .env websocket-server
