import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
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
                            { text: '什么是 RedisShake', link: '/zh/guide/getting-started' },
                            { text: '快速上手', link: '/zh/guide/getting-started' },
                            { text: '配置', link: '/zh/guide/getting-started' },
                            { text: '迁移模式选择', link: '/zh/guide/getting-started' },
                        ]
                    },
                    {
                        text: 'Reader',
                        items: [
                            { text: 'Sync Reader', link: '/zh/function/introduction' },
                            { text: 'Scan Reader', link: '/zh/function/best_practices' },
                            { text: 'RDB Reader', link: '/zh/function/best_practices' },
                        ]
                    },
                    {
                        text: 'Writer',
                        items: [
                            { text: 'Redis Writer', link: '/zh/function/introduction' },
                        ]
                    },
                    {
                        text: 'function',
                        items: [
                            { text: '什么是 function', link: '/zh/function/introduction' },
                            {
                                text: '最佳实践',
                                items: [
                                    { text: '监控', link: '/zh/function/best_practices' },
                                ]
                            }
                        ]
                    },
                    {
                        text: '进阶用法',
                        items: [
                            { text: '监控', link: '/zh/function/best_practices' },
                            { text: '双向同步', link: '/zh/function/best_practices' },
                            { text: '容器部署', link: '/zh/function/best_practices' },
                            { text: '主从实例向集群实例迁移', link: '/zh/function/best_practices' },
                            { text: '大 key 重写', link: '/zh/function/best_practices' },
                        ]
                    }
                ],
                footer: {
                    message: 'Released under the MIT License.',
                    copyright: 'Copyright © 2019-present Tair'
                }
            }
        },
        en: {
            label: 'English',
            lang: 'en',
        },
    },
    themeConfig: {
        socialLinks: [
            { icon: 'github', link: 'https://github.com/tair-opensource/RedisShake' }
        ],
    }
})
