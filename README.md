# ticketmaster2kafka

Create an env file with the following parameters:
- KAFKA_BOOTSTRAP
- KAFKA_USER
- KAFKA_PASSWORD

The Strimzi CA cert must live on the host at `/etc/viewlive/certs/strimzi-ca.crt`
and is mounted read-only into the container. Override with `KAFKA_CA_LOCATION`
if you need a different in-container path.

Use that env file in the following:
```
docker build -t ticketmaster2kafka:0.0 .
docker run \
  --env-file path/to/1.env \
  -v /etc/viewlive/certs:/etc/viewlive/certs:ro \
  ticketmaster2kafka:0.0
```


