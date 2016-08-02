# MassTransit-Stress

This console application is designed to test RabbitMQ under load. 

# Usage

```
	MassTransit.StressConsole [options]
```

# Command Line Options


## Security

The username/password for the server can be specified using the following options:

```
	-rmqusername "username" -rmqpassword "password"
```

## Server URI

The URI for the server can be specified. In the example below, the ```loadtest_queue``` is used on the ```vortex``` 
virtual host on the local server with a message prefetch count of 32.


```
	-uri "rabbitmq://localhost/vortex/loadtest_queue&prefetch=32"
```

## Heartbeat

The server connection heartbeat can be specifed:

```
	-heartbeat:3
```

## Test Parameters

The number of iterations and the number of instances can be adjusted as well, to increase the duration or load on the server.

```
	-iterations:10000 -instances:10
```

### Defaults

The default configuration includes:

	Username: guest
	Password: guest
	Uri: rabbitmq://localhost/stress_service?prefetch=32
	Heartbeat: 3
	Iterations: 1000
	Instances: 10


