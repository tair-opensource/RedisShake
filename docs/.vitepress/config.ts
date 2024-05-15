import { defineConfigWithTheme } from "vitepress"
import { en } from "./en"
import { zh } from "./zh"

// https://vitepress.dev/reference/site-config
export default defineConfigWithTheme({
    head: [["link", { rel: "icon", href: "/RedisShake/favicon.ico" }]],
    base: "/RedisShake/",
    title: "RedisShake",
    srcDir: "./src",

    locales: {
        zh: { label: "简体中文", ...zh },
        en: { label: "English", ...en },
    },
    themeConfig: {
        nav: zh.themeConfig?.nav,
        socialLinks: [
            { icon: "github", link: "https://github.com/tair-opensource/RedisShake" }
        ],
        editLink: {
            pattern: "https://github.com/tair-opensource/RedisShake/tree/v4/docs/src/:path"
        },
        lastUpdated: {
            text: "Updated at",
            formatOptions: {
                dateStyle: "full",
                timeStyle: "medium"
            }
        },
        footer: {
            message: "Released under the MIT License.",
            copyright: "Copyright © 2019-present Tair"
        }
    }
})
