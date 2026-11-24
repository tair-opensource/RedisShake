import fs from 'node:fs'
import path from 'node:path'
import type { SiteConfig } from 'vitepress'

const SITE_ORIGIN = 'https://tair-opensource.github.io/RedisShake'

interface DocEntry {
    title: string
    section: string
    relPath: string
    summary: string
    content: string
}

const ORDERED_SECTIONS: Array<{
    enSection: string
    zhSection: string
    files: string[]
}> = [
    {
        enSection: 'Introduction',
        zhSection: '介绍',
        files: [
            'guide/introduction.md',
            'guide/getting-started.md',
            'guide/mode.md',
            'guide/config.md',
        ],
    },
    {
        enSection: 'Reader',
        zhSection: 'Reader',
        files: [
            'reader/sync_reader.md',
            'reader/scan_reader.md',
            'reader/rdb_reader.md',
            'reader/aof_reader.md',
        ],
    },
    {
        enSection: 'Writer',
        zhSection: 'Writer',
        files: [
            'writer/redis_writer.md',
            'writer/file_writer.md',
        ],
    },
    {
        enSection: 'Filter and Processing',
        zhSection: '过滤与加工',
        files: [
            'filter/filter.md',
            'filter/function.md',
        ],
    },
    {
        enSection: 'Operations and Reference',
        zhSection: '生产实践与参考',
        files: [
            'guide/architecture.md',
            'others/compatibility.md',
            'others/version.md',
            'others/consistent.md',
            'others/checkpoint.md',
            'others/modules.md',
        ],
    },
]

function stripFrontmatter(raw: string): string {
    return raw.replace(/^---\r?\n[\s\S]*?\r?\n---\r?\n*/, '').trim()
}

function extractTitle(body: string, fallback: string): string {
    const match = body.match(/^#\s+(.+)$/m)
    return match ? match[1].trim() : fallback
}

function extractSummary(body: string): string {
    const lines = body
        .split(/\r?\n/)
        .map((l) => l.trim())
        .filter((l) => l && !l.startsWith('#') && !l.startsWith('!') && !l.startsWith('```') && !l.startsWith(':::'))
    const first = lines[0] || ''
    return first.length > 140 ? `${first.slice(0, 137)}...` : first
}

function collectDocs(srcDir: string, lang: 'en' | 'zh'): DocEntry[] {
    const entries: DocEntry[] = []
    for (const group of ORDERED_SECTIONS) {
        const section = lang === 'en' ? group.enSection : group.zhSection
        for (const file of group.files) {
            const absPath = path.join(srcDir, lang, file)
            if (!fs.existsSync(absPath)) continue
            const raw = fs.readFileSync(absPath, 'utf-8')
            const body = stripFrontmatter(raw)
            const title = extractTitle(body, path.basename(file, '.md'))
            const summary = extractSummary(body)
            entries.push({
                title,
                section,
                relPath: `${lang}/${file}`,
                summary,
                content: body,
            })
        }
    }
    return entries
}

function copyRawMarkdown(srcDir: string, outDir: string, entries: DocEntry[]): void {
    for (const entry of entries) {
        const dest = path.join(outDir, entry.relPath)
        fs.mkdirSync(path.dirname(dest), { recursive: true })
        fs.copyFileSync(path.join(srcDir, entry.relPath), dest)
    }
}

function buildIndex(enDocs: DocEntry[], zhDocs: DocEntry[]): string {
    const lines: string[] = [
        '# RedisShake',
        '',
        '> Zero-downtime Valkey & Redis data migration, filtering, and transformation tool supporting Redis 2.8–8.12.x, Valkey 8.x–9.2.x, Alibaba Cloud Tair, AWS ElastiCache, and AWS MemoryDB.',
        '',
        '- Full documentation (English, single file): ' + `${SITE_ORIGIN}/llms-full.txt`,
        '- Full documentation (Chinese, single file): ' + `${SITE_ORIGIN}/llms-full-zh.txt`,
        '- Repository: https://github.com/tair-opensource/RedisShake',
        '- Releases: https://github.com/tair-opensource/RedisShake/releases',
        '',
    ]

    const renderGroups = (docs: DocEntry[], headingPrefix: string) => {
        const bySection = new Map<string, DocEntry[]>()
        for (const doc of docs) {
            const list = bySection.get(doc.section) || []
            list.push(doc)
            bySection.set(doc.section, list)
        }
        for (const [section, list] of bySection.entries()) {
            lines.push(`## ${headingPrefix}${section}`, '')
            for (const doc of list) {
                const url = `${SITE_ORIGIN}/${doc.relPath}`
                const desc = doc.summary ? `: ${doc.summary}` : ''
                lines.push(`- [${doc.title}](${url})${desc}`)
            }
            lines.push('')
        }
    }

    renderGroups(enDocs, '')
    renderGroups(zhDocs, '中文 — ')

    return lines.join('\n')
}

function buildFull(docs: DocEntry[], lang: 'en' | 'zh'): string {
    const header =
        lang === 'en'
            ? [
                  '# RedisShake Full Documentation (English)',
                  '',
                  '> Generated automatically from official RedisShake v4 documentation.',
                  `> Source: ${SITE_ORIGIN}/en/`,
                  '',
              ]
            : [
                  '# RedisShake 完整文档（简体中文）',
                  '',
                  '> 由 RedisShake v4 官方文档站自动构建生成。',
                  `> 来源：${SITE_ORIGIN}/`,
                  '',
              ]

    const chunks = docs.map(
        (doc) =>
            `---\n<!-- source: ${SITE_ORIGIN}/${doc.relPath} -->\n\n${doc.content}`
    )
    return `${header.join('\n')}\n${chunks.join('\n\n')}\n`
}

export function generateLlmsFiles(siteConfig: SiteConfig): void {
    const { srcDir, outDir } = siteConfig
    const enDocs = collectDocs(srcDir, 'en')
    const zhDocs = collectDocs(srcDir, 'zh')

    copyRawMarkdown(srcDir, outDir, enDocs)
    copyRawMarkdown(srcDir, outDir, zhDocs)

    fs.writeFileSync(path.join(outDir, 'llms.txt'), buildIndex(enDocs, zhDocs), 'utf-8')
    fs.writeFileSync(path.join(outDir, 'llms-full.txt'), buildFull(enDocs, 'en'), 'utf-8')
    fs.writeFileSync(path.join(outDir, 'llms-full-zh.txt'), buildFull(zhDocs, 'zh'), 'utf-8')
}
