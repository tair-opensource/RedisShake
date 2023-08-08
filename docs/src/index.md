---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
    name: "RedisShake"
    # text: "用于 Redis-like 数据库的数据迁移与处理服务"
    tagline: 用于 Redis-like 数据库的数据迁移与处理服务
    actions:
        -   theme: brand
            text: 快速上手
            link: /zh/guide/getting-started
        # -   theme: alt
        #     text: 云原生内存数据库Tair
        #     link: https://www.aliyun.com/product/apsaradb/kvstore/tair

features:
    -   title: 数据迁移
        details: 支持 sync、scan 和 restore 三种数据迁移模式
    -   title: 数据处理
        details: 支持使用 lua 脚本对数据进行过滤与修改
    -   title: 云数据库支持
        details: 兼容主流云厂商的多种架构：主从、集群等
---

