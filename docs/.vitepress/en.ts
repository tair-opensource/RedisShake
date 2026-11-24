import { defineConfig, type DefaultTheme } from 'vitepress'

export const en = defineConfig({
    lang: 'en-US',
    description: 'Zero-downtime Valkey & Redis data migration, filtering, and transformation tool.',

    themeConfig: {
        nav: nav(),
        sidebar: {
            '/en/': sidebar(),
        },
        outline: {
            level: [2, 3],
        },
        editLink: {
            pattern: 'https://github.com/tair-opensource/RedisShake/edit/v4/docs/src/:path',
            text: 'Edit this page on GitHub',
        },
    },
})

function nav(): DefaultTheme.NavItem[] {
    return [
        { text: 'Home', link: '/en/' },
        { text: 'Documentation', link: '/en/guide/getting-started', activeMatch: '/en/(guide|reader|writer|filter|others)/' },
        { text: 'Tair', link: 'https://www.alibabacloud.com/product/tair' },
    ]
}

function sidebar(): DefaultTheme.SidebarItem[] {
    return [
        {
            text: 'Introduction',
            items: [
                { text: 'What is RedisShake', link: '/en/guide/introduction' },
                { text: 'Getting Started', link: '/en/guide/getting-started' },
                { text: 'Migration Mode Selection', link: '/en/guide/mode' },
                { text: 'Configuration', link: '/en/guide/config' },
            ],
        },
        {
            text: 'Reader',
            items: [
                { text: 'Sync Reader', link: '/en/reader/sync_reader' },
                { text: 'Scan Reader', link: '/en/reader/scan_reader' },
                { text: 'RDB Reader', link: '/en/reader/rdb_reader' },
                { text: 'AOF Reader', link: '/en/reader/aof_reader' },
            ],
        },
        {
            text: 'Writer',
            items: [
                { text: 'Redis Writer', link: '/en/writer/redis_writer' },
                { text: 'File Writer', link: '/en/writer/file_writer' },
            ],
        },
        {
            text: 'Filter and Processing',
            items: [
                { text: 'Built-in Filter Rules', link: '/en/filter/filter' },
                { text: 'What is function', link: '/en/filter/function' },
            ],
        },
        {
            text: 'Operations and Reference',
            items: [
                { text: 'Architecture and Performance', link: '/en/guide/architecture' },
                { text: 'Version Compatibility', link: '/en/others/compatibility' },
                { text: 'Cross-version Migration', link: '/en/others/version' },
                { text: 'How to Verify Data Consistency', link: '/en/others/consistent' },
                { text: 'Checkpoint', link: '/en/others/checkpoint' },
                { text: 'Redis Modules', link: '/en/others/modules' },
            ],
        },
    ]
}
