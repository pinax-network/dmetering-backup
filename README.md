# StreamingFast Metering Library

This is a fork of StreamingFast's dmetering package, which can be found [here](https://github.com/streamingfast/dmetering).

The fork adds a Redis Pub/Sub emitter for accumulated events. The protobuf for emitted events can be found in the 
[dtypes](https://github.com/pinax-network/dtypes) package. An exemplary Redis subscriber with an Elastic sink can be 
found in the [dmetering-elastic](https://github.com/pinax-network/dmetering-elastic) repository.

## Usage

See example usage in [dgraphql](https://github.com/streamingfast/dgraphql).

The following plugins are provided by this package: (feel free to implement your own)

* `null://`