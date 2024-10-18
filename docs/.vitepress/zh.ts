import { createRequire } from 'module'
import { defineConfig, type DefaultTheme } from 'vitepress'

const require = createRequire(import.meta.url)
const pkg = require('vitepress/package.json')

export const zh = defineConfig({
    lang: 'zh-Hans',
    themeConfig: {
        nav: nav(),
        sidebar: sidebar(),
    }
})

function nav(): DefaultTheme.NavItem[] {
    return [
        { text: '主页', link: '/' },
        { text: '使用文档', link: '/zh/guide/getting-started' },
        { text: '云原生内存数据库 Tair', link: 'https://www.aliyun.com/product/apsaradb/kvstore/tair' }
    ]
}

function sidebar(): DefaultTheme.SidebarItem[] {
    return [
        {
            text: '介绍',
            items: [
                { text: '什么是 RedisShake', link: '/zh/guide/introduction' },
                { text: '快速上手', link: '/zh/guide/getting-started' },
                { text: '配置', link: '/zh/guide/config' },
                { text: '迁移模式选择', link: '/zh/guide/mode' },
                { text: '架构与性能说明', link: '/zh/guide/architecture' },
            ]
        },
        {
            text: 'Reader',
            items: [
                { text: 'Sync Reader', link: '/zh/reader/sync_reader' },
                { text: 'Scan Reader', link: '/zh/reader/scan_reader' },
                { text: 'RDB Reader', link: '/zh/reader/rdb_reader' },
            ]
        },
        {
            text: 'Writer',
            items: [
                { text: 'Redis Writer', link: '/zh/writer/redis_writer' },
            ]
        },
        {
            text: '过滤与加工',
            items: [
                { text: '内置过滤规则', link: '/zh/filter/filter' },
                { text: '什么是 function', link: '/zh/filter/function' },
            ]
        },
        {
            text: 'Others',
            items: [
                { text: 'Redis Modules', link: '/zh/others/modules' },
                { text: '如何判断数据一致', link: '/zh/others/consistent' },
                { text: '跨版本迁移', link: '/zh/others/version' },
            ]
        },
    ]
}
