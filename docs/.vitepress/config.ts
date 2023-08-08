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
                        text: '基础教程',
                        items: [
                            { text: 'RedisShake 简介', link: '/zh/guide/getting-started' },
                            { text: '快速上手', link: '/zh/guide/getting-started' },
                            { text: '配置文件', link: '/zh/guide/config' }
                        ]
                    },
                    {
                        text: '变换/过滤',
                        items: [
                            { text: '上手使用', link: '/zh/transform/getting-started' },
                            { text: '样例', link: '/zh/transform/examples' }
                        ]
                    }
                ],
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
