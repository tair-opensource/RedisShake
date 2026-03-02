# Resumable Transfer (Checkpoint)

RedisShake 4.x **does NOT support resumable transfer** or cluster topology change awareness.

Resumable transfer means a data synchronization tool can continue from where it was interrupted. For example, Alibaba Cloud DTS (Data Transmission Service) and Tair Global Active-Active both support resumable transfer - sync tasks can resume from the checkpoint after restart, and automatically recover from network fluctuations or node failovers.

However, implementing production-grade resumable transfer and topology awareness is **extremely complex**, requiring **Redis itself, the sync tool, and surrounding components** to be modified together for robust implementation. RedisShake alone cannot achieve this. This is why RedisShake adopts a "stateless synchronization" and "static topology" design.

Therefore, issues such as network interruption, process crash, server restart, cluster scaling, and failover may cause synchronization to terminate, requiring a full resync from the beginning after restart.

**Suitable Scenarios**: RedisShake is suitable for one-time data migration, not for long-term synchronization scenarios that require high availability. For the latter, consider using commercial products like **Alibaba Cloud DTS** or **Tair Global Active-Active**, which provide complete resumable transfer and topology awareness capabilities.
