# Kiwi
![](docs/images/Kiwi-logo.png)
[中文](README_CN.md)

A C++20 implementation of Redis Server, use RocksDB for persist storage.(not including cluster yet)

## Requirements

* Linux or OS X or FreeBSD

```shell
# Rocky Linux, Fedora Linux, CentOS
sudo dnf groupinstall -y 'Development Tools'
sudo dnf install cmake g++ autoconf perl -y
git config --global http.version HTTP/1.1
# Ubuntu
sudo apt install cmake g++ autoconf perl -y
git config --global http.version HTTP/1.1
```

## Compile

Execute this command to start compiling Kiwi:

```bash
./etc/script/build.sh 
```

Kiwi is compiled by default in release mode, which does not support debugging. If debugging is needed, compile in debug mode.

```bash
./etc/script/build.sh --clear
./etc/script/build.sh --debug
```

## Run

```bash
./bin/kiwi ./etc/conf/Kiwi.conf
```

## Support module for write your own extensions
 Kiwi supports module now, still in progress, much work to do.
 I added three commands(ldel, skeys, hgets) for demonstration.

## Persistence: Not limited to memory
 RocksDB can be configured as backend for Kiwi.

## Fully compatible with redis
 You can test Kiwi with redis-cli, redis-benchmark, or use redis as master with Kiwi as slave or conversely, it also can work with redis sentinel.

## High Performance
- Kiwi is approximately 20-25% faster than redis if run benchmark with pipeline requests(set -P = 50 or higher).
- Average 80K requests per seconds for write, and 90K requests per seconds for read.
- Before run test, please ensure that std::list::size() is O(1), obey the C++11 standards.

Run this command, compare with redis use pipeline commands, try it.
```bash
./redis-benchmark -q -n 1000000 -P 50 -c 50
```

## Command List
#### show all supported commands list
- cmdlist

#### key commands
- type exists del expire pexpire expireat pexpireat ttl pttl persist move keys randomkey rename renamenx scan sort

#### server commands
- select dbsize bgsave save lastsave flushdb flushall client debug shutdown bgrewriteaof ping echo info monitor auth

#### string commands
- set get getrange setrange getset append bitcount bitop getbit setbit incr incrby incrbyfloat decr decrby mget mset msetnx setnx setex psetex strlen

#### list commands
- lpush rpush lpushx rpushx lpop rpop lindex llen lset ltrim lrange linsert lrem rpoplpush blpop brpop brpoplpush

#### hash commands
- hget hmget hgetall hset hsetnx hmset hlen hexists hkeys hvals hdel hincrby hincrbyfloat hscan hstrlen

#### set commands
- sadd scard srem sismember smembers sdiff sdiffstore sinter sinterstore sunion sunionstore smove spop srandmember sscan

#### sorted set commands
- zadd zcard zrank zrevrank zrem zincrby zscore zrange zrevrange zrangebyscore zrevrangebyscore zremrangebyrank zremrangebyscore zpopmin zpopmax zunionstore zinterstore

#### pubsub commands
- subscribe unsubscribe publish psubscribe punsubscribe pubsub

#### multi commands
- watch unwatch multi exec discard

#### replication commands
- sync slaveof

## Contact Us

![](docs/images/Kiwi-wechat.png)

