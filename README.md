# ZooKeepeRS

Work in progress.

A Rust ZooKeeper client library and some command-line utilities to explore and analyze ZooKeeper's persistent data.

## FAQ

* Why the name?

  ZooKeeper's motto is *"[Because coordinating distributed systems is a Zoo](https://zookeeper.apache.org/doc/r3.5.5/)"*. Now the zoo needs more than one keeper, and they'd better not be rusty!
  
* Why a new project when we already have [rust-zookeeper](https://github.com/bonifaido/rust-zookeeper) and [tokio-zookeeper](https://github.com/jonhoo/tokio-zookeeper)?

  Rust-zookeeper uses blocking I/O. Tokio-zookeeper is based on the now-deprecated futures-0.1 and is developped as part of an ongoing series of live streaming coding sessions. ZooKeepeRS ~~uses~~ will use modern async/await and is not just a client, providing types and tools to explore ZooKeeper's persistent data.


## License

Apache Licence 2.0

By contributing to this repository you implicitly accept your contributions to be licensed under the Apache License 2.0
