# How to Verify Data Consistency

During the incremental synchronization phase, it's often necessary to determine if the data between the source and target is consistent. Here are two empirical methods:

1. By observing logs or monitoring, when RedisShake's synchronization traffic reaches 0, it's considered that synchronization has ended and both ends are consistent.
2. Compare the number of keys between the source and destination. If they are equal, it's considered consistent.

However, if keys have expiration times, it can lead to inconsistencies in key counts. Reasons include:

1. Due to limitations in the expiration algorithm, some keys in the source may have expired but not actually been deleted. These keys might be deleted in the destination, causing the destination to have fewer keys than the source.
2. The source and destination run independently, each with its own expiration algorithm. The randomness in these algorithms can cause inconsistencies in which keys are deleted, leading to different key counts.

In practice, keys with expiration times are generally considered acceptable to be inconsistent and won't affect business operations. Therefore, you can verify consistency by checking only the number of keys without expiration times. As shown below, you should calculate and compare the value of `$keys-$expires` for both the source and destination. [[795]](https://github.com/tair-opensource/RedisShake/issues/795) [[791]](https://github.com/tair-opensource/RedisShake/issues/791)

```
127.0.0.1:6379> info keyspace
# Keyspace
db0:keys=4463175,expires=2,avg_ttl=333486
```

