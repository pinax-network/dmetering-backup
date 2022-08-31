# StreamingFast Metering Library

This is a fork of StreamingFast's dmetering package, which can be found [here](https://github.com/streamingfast/dmetering).

The fork adds a Redis Pub/Sub emitter for accumulated events. The protobuf for emitted events can be found in the 
[dtypes](https://github.com/pinax-network/dtypes) package. An exemplary Redis subscriber with an Elastic sink can be 
found in the [dmetering-elastic](https://github.com/pinax-network/dmetering-elastic) repository.

## Usage

See example usage in [dgraphql](https://github.com/streamingfast/dgraphql).

The following plugins are provided by this package:

* `null://` (drops all metering events)
* `zlog://` (writes all events to the configured log)
* `redis://` (writes all events in to redis pub/sub)

### Redis

Plugin url format: `redis://<hosts>:<port>/<topic>?network=<network_id>&emitterDelay=10s&warnOnErrors=true&masterName=<redis_master>` with:

* `hosts`: list of comma separated redis hosts
* `port`: port of the redis nodes (currently only possible to specify one port for all nodes)
* `topic`: topic to listen on redis pub/sub
* `network`: the network id this firehose instance is running on
* `emitterDelay`: how often to emit accumulated events into redis
* `warnOnErrors`: whether to log redis errors
* `masterName`: redis master name

Example url: `redis://10.1.0.1,10.1.0.2,10.1.0.3:2367/eth?network=eth&emitterDelay=10s&warnOnErrors=true&masterName=mymaster`