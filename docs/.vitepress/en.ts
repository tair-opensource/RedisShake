import { createRequire } from 'module'
import { defineConfig, type DefaultTheme } from 'vitepress'

const require = createRequire(import.meta.url)
const pkg = require('vitepress/package.json')

export const en = defineConfig({
    lang: 'en-US',
    description: 'Vite & Vue powered static site generator.',

    themeConfig: {
        nav: nav(),
        sidebar: sidebar(),
    }
})

function nav(): DefaultTheme.NavItem[] {
    return [
        { text: 'Home', link: '/en/' },
        { text: 'User Guide', link: '/en/guide/getting-started' },
        { text: 'Tair', link: 'https://www.alibabacloud.com/product/tair' }
    ]
}

function sidebar(): DefaultTheme.SidebarItem[] {
    return [
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
    ]
}
