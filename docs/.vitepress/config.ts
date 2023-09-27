import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
    head: [['link', { rel: 'icon', href: '/RedisShake/favicon.ico' }]],
    base: "/RedisShake/",
    title: "RedisShake",
    description: "RedisShake is a tool for processing and migrating Redis data.",
    srcDir: './src',
    locales: {
        root: {
            label: '中文',
            lang: 'zh', // optional, will be added  as `lang` attribute on `html` tag
            themeConfig: {
                // https://vitepress.dev/reference/default-theme-config
                nav: [
                    { text: '主页', link: '/' },
                    { text: '使用文档', link: '/zh/guide/getting-started' },
                    { text: '云原生内存数据库 Tair', link: 'https://www.aliyun.com/product/apsaradb/kvstore/tair' }
                ],
                sidebar: [
                    {
                        text: '介绍',
                        items: [
                            { text: '什么是 RedisShake', link: '/zh/guide/introduction' },
                            { text: '快速上手', link: '/zh/guide/getting-started' },
                            { text: '配置', link: '/zh/guide/config' },
                            { text: '迁移模式选择', link: '/zh/guide/mode' },
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
                        text: 'Function',
                        items: [
                            { text: '什么是 function', link: '/zh/function/introduction' },
                            { text: '最佳实践', link: '/zh/function/best_practices' }
                        ]
                    },
                    {
                        text: 'Others',
                        items: [
                            { text: 'Redis Modules', link: '/zh/others/modules' },
                        ]
                    },
                ],
                footer: {
                    message: 'Released under the MIT License.',
                    copyright: 'Copyright © 2019-present Tair'
                }
            }
        },
        en: {
            label: 'English',
            lang: 'en', // optional, will be added as `lang` attribute on `html` tag
            themeConfig: {
                // https://vitepress.dev/reference/default-theme-config
                nav: [
                    { text: 'Home', link: '/en/' },
                    { text: 'User Guide', link: '/en/guide/getting-started' },
                    { text: 'Tair', link: 'https://www.alibabacloud.com/product/tair' }
                ],
                sidebar: [
                    {
                        text: 'Introduction',
                        items: [
                            { text: 'What is RedisShake', link: '/en/guide/introduction' },
                            { text: 'Getting Started', link: '/en/guide/getting-started' },
                            { text: 'Configuration', link: '/en/guide/config' },
                            { text: 'Migration Mode Selection', link: '/en/guide/mode' },
                        ]
                    },
                    {
                        text: 'Reader',
                        items: [
                            { text: 'Sync Reader', link: '/en/reader/sync_reader' },
                            { text: 'Scan Reader', link: '/en/reader/scan_reader' },
                            { text: 'RDB Reader', link: '/en/reader/rdb_reader' },
                        ]
                    },
                    {
                        text: 'Writer',
                        items: [
                            { text: 'Redis Writer', link: '/en/writer/redis_writer' },
                        ]
                    },
                    {
                        text: 'Function',
                        items: [
                            { text: 'What is function', link: '/en/function/introduction' },
                            { text: 'Best Practices', link: '/en/function/best_practices' }
                        ]
                    },
                    {
                        text: 'Others',
                        items: [
                            { text: 'Redis Modules', link: '/en/others/modules' },
                        ]
                    },
                ],
                footer: {
                    message: 'Released under the MIT License.',
                    copyright: 'Copyright © 2019-present Tair'
                }
            }
        },

    },
    themeConfig: {
        socialLinks: [
            { icon: 'github', link: 'https://github.com/tair-opensource/RedisShake' }
        ],
    }
})
