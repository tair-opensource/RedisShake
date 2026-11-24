---
layout: home

hero:
  name: RedisShake
  text: Valkey / Redis 数据迁移与加工
  tagline: 支持多版本多架构不停机迁移、规则过滤与 Lua 实时加工，覆盖自建与主流云数据库。
  actions:
    - theme: brand
      text: 快速上手
      link: /zh/guide/getting-started
    - theme: alt
      text: 迁移模式选择
      link: /zh/guide/mode
    - theme: alt
      text: GitHub
      link: https://github.com/tair-opensource/RedisShake

features:
  - icon: 🔀
    title: 多版本多架构兼容
    details: 兼容 Redis 2.8–8.12.x 与 Valkey 8.x–9.2.x，覆盖单机、主从、哨兵与集群，并支持阿里云 Tair、AWS ElastiCache 与 MemoryDB。
  - icon: ⚡
    title: 不停机在线迁移
    details: 支持通过 PSync 与 Scan 模式在线迁移数据，业务无需停机，满足不同环境的源端接入需求。
  - icon: 🧩
    title: 内置过滤与 Lua 加工
    details: 支持 Key / DB / Command / Slot 规则过滤，并可通过 Lua Function 灵活改写数据。
  - icon: 📦
    title: 离线快照与文件导出
    details: 支持从 RDB / AOF 文件恢复与闪回，也支持通过 File Writer 导出为 AOF、CMD 或 JSON。
---

<div class="home-section">

## 架构一览

<div class="arch-diagram">

![RedisShake Pipeline Architecture](/intro.svg)

</div>

## 30 秒快速体验

```bash
docker run --rm --network host \
  -e SYNC=true \
  -e SHAKE_SRC_ADDRESS=127.0.0.1:6379 \
  -e SHAKE_DST_ADDRESS=127.0.0.1:6380 \
  ghcr.io/tair-opensource/redisshake:latest
```

</div>
