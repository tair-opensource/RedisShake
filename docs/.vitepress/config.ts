import { defineConfig } from 'vitepress'
import { en } from './en'
import { zh } from './zh'
import { generateLlmsFiles } from './llms'

// https://vitepress.dev/reference/site-config
export default defineConfig({
    head: [
        ['link', { rel: 'icon', href: '/RedisShake/favicon.ico' }],
        ['meta', { name: 'theme-color', content: '#2563eb' }],
    ],
    base: '/RedisShake/',
    title: 'RedisShake',
    description: 'Zero-downtime Valkey & Redis data migration, filtering, and transformation tool.',
    srcDir: './src',
    cleanUrls: true,
    lastUpdated: true,

    locales: {
        root: { label: '简体中文', ...zh },
        en: { label: 'English', ...en },
    },

    themeConfig: {
        search: {
            provider: 'local',
        },
        socialLinks: [
            { icon: 'github', link: 'https://github.com/tair-opensource/RedisShake' },
        ],
        editLink: {
            pattern: 'https://github.com/tair-opensource/RedisShake/edit/v4/docs/src/:path',
        },
        footer: {
            message: 'Released under the MIT License.',
            copyright: 'Copyright © 2019-present Tair Team',
        },
    },

    buildEnd: generateLlmsFiles,
})
