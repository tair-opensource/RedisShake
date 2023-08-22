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
        -   theme: alt
            text: 什么是 RedisShake
            link: /zh/guide/introduction
features:
    -   title: 数据迁移
        details: 支持 sync、scan 和 restore 三种数据迁移模式
    -   title: 数据加工
        details: 支持使用 lua 脚本对数据进行过滤与修改
    -   title: 兼容
        details: 兼容多种 Redis 部署形态，兼容主流云厂商的 Redis-like 数据库
---

