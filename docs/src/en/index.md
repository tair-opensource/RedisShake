---
layout: home

hero:
  name: RedisShake
  text: Valkey & Redis Migration Tool
  tagline: Multi-version, multi-topology online migration with built-in filtering and Lua transformation.
  actions:
    - theme: brand
      text: Get Started
      link: /en/guide/getting-started
    - theme: alt
      text: Choose a Mode
      link: /en/guide/mode
    - theme: alt
      text: GitHub
      link: https://github.com/tair-opensource/RedisShake

features:
  - icon: 🔀
    title: Multi-Version & Multi-Topology
    details: Supports Redis 2.8–8.12.x and Valkey 8.x–9.2.x across standalone, replica, sentinel, and cluster, plus Alibaba Cloud Tair and AWS ElastiCache/MemoryDB.
  - icon: ⚡
    title: Zero-Downtime Migration
    details: Migrate live workloads without interruption using either PSync or Scan readers to fit different source environments.
  - icon: 🧩
    title: Filter & Lua Transform
    details: Filter by key, prefix, regex, DB, command, or slot, and rewrite commands on the fly with Lua Functions.
  - icon: 📦
    title: Offline Files & Export
    details: Replay RDB and AOF backups for recovery or flashback, or export streams to AOF, CMD, and JSON files.
---

<div class="home-section">

## Architecture

<div class="arch-diagram">

![RedisShake Pipeline Architecture](/intro.svg)

</div>

## 30-Second Quick Start

```bash
docker run --rm --network host \
  -e SYNC=true \
  -e SHAKE_SRC_ADDRESS=127.0.0.1:6379 \
  -e SHAKE_DST_ADDRESS=127.0.0.1:6380 \
  ghcr.io/tair-opensource/redisshake:latest
```

</div>
