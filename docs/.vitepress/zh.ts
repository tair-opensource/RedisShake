import { defineConfig, type DefaultTheme } from 'vitepress'

export const zh = defineConfig({
    lang: 'zh-Hans',
    description: '面向 Valkey 与 Redis 的零停机数据迁移、过滤与加工工具',

    themeConfig: {
        nav: nav(),
        sidebar: {
            '/zh/': sidebar(),
        },
        outline: {
            label: '本页目录',
            level: [2, 3],
        },
        editLink: {
            pattern: 'https://github.com/tair-opensource/RedisShake/edit/v4/docs/src/:path',
            text: '在 GitHub 上编辑此页',
        },
        docFooter: {
            prev: '上一页',
            next: '下一页',
        },
        lastUpdated: {
            text: '最后更新于',
        },
        returnToTopLabel: '回到顶部',
        sidebarMenuLabel: '目录',
        darkModeSwitchLabel: '主题',
    },
})

function nav(): DefaultTheme.NavItem[] {
    return [
        { text: '主页', link: '/' },
        { text: '使用文档', link: '/zh/guide/getting-started', activeMatch: '/zh/' },
        { text: '云原生内存数据库 Tair', link: 'https://www.aliyun.com/product/apsaradb/kvstore/tair' },
    ]
}

function sidebar(): DefaultTheme.SidebarItem[] {
    return [
        {
            text: '介绍',
            items: [
                { text: '什么是 RedisShake', link: '/zh/guide/introduction' },
                { text: '快速上手', link: '/zh/guide/getting-started' },
                { text: '迁移模式选择', link: '/zh/guide/mode' },
                { text: '配置', link: '/zh/guide/config' },
            ],
        },
        {
            text: 'Reader',
            items: [
                { text: 'Sync Reader', link: '/zh/reader/sync_reader' },
                { text: 'Scan Reader', link: '/zh/reader/scan_reader' },
                { text: 'RDB Reader', link: '/zh/reader/rdb_reader' },
                { text: 'AOF Reader', link: '/zh/reader/aof_reader' },
            ],
        },
        {
            text: 'Writer',
            items: [
                { text: 'Redis Writer', link: '/zh/writer/redis_writer' },
                { text: 'File Writer', link: '/zh/writer/file_writer' },
            ],
        },
        {
            text: '过滤与加工',
            items: [
                { text: '内置过滤规则', link: '/zh/filter/filter' },
                { text: '什么是 function', link: '/zh/filter/function' },
            ],
        },
        {
            text: '生产实践与参考',
            items: [
                { text: '架构与性能说明', link: '/zh/guide/architecture' },
                { text: '版本兼容性', link: '/zh/others/compatibility' },
                { text: '跨版本迁移', link: '/zh/others/version' },
                { text: '如何判断数据一致', link: '/zh/others/consistent' },
                { text: '断点续传', link: '/zh/others/checkpoint' },
                { text: 'Redis Modules', link: '/zh/others/modules' },
            ],
        },
    ]
}
