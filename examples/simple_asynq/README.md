To run the asynq UI server, run the following command:
```bash
docker run --rm --name asynqmon  -p 3000:3000 hibiken/asynqmon:0.2 --port=3000 --redis-addr=host.docker.internal:6379
```