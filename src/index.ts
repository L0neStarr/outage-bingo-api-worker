import { parseFeed } from '@rowanmanning/feed-parser'

interface Env {
  BINGO_BUCKET: R2Bucket
  outage_bingo_kv: KVNamespace
}

function padMM(mm: number) {
  return String(mm).padStart(2, "0")
}

function buildKey() {
  const date = new Date()
  const yyyy = date.getUTCFullYear()
  const mm = padMM(date.getUTCMonth() + 1)
  return `outages-${yyyy}-${mm}.json`
}



export default {
  async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    switch (controller.cron) {
      case "0,30 * 1 * *": {
        // Build monthly template file if not present
        const key = buildKey()

        if (!(await env.BINGO_BUCKET.head(key))) {
          const template = await env.BINGO_BUCKET.get("outages-template.json")
          if (!template) throw new Error("Missing outages-template.json")

          // Copies template into monthly card into R2 storage
          await env.BINGO_BUCKET.put(key, template.body, { httpMetadata: { contentType: "application/json" } })
        }

        break
      }

      case "0 * * * *": {
        // Hourly API check (currently only implementing "api_atlassian" parsing + updating monthly JSON object)

        // Load outage-sources.json
        const sourceTemplate = await env.BINGO_BUCKET.get("outage-sources.json")
        if (!sourceTemplate) throw new Error("Missing outage-sources.json")
        const sourcesText = await sourceTemplate.text()
        const sourcesObj = JSON.parse(sourcesText) as any


        // Load monthly outages-YYYY-MM.json into memory
        const mmKey = buildKey()
        const mmTemplate = await env.BINGO_BUCKET.get(mmKey)
        if (!mmTemplate) throw new Error(`Missing ${mmKey} in R2 (monthly file not created yet)`)
        const mmData = await mmTemplate.text()
        const mmObj = JSON.parse(mmData) as MonthlyOutageBingo[]

        // Map vendor name to index in mmObj so updates target the correct vendor (prevents overwriting all vendors)
        const mmIndex = new Map<string, number>()
        for (let i = 0; i < mmObj.length; i++) mmIndex.set(mmObj[i].name, i)

        // Append shortlink if vendor exists and link isn't already present
		  function appendLink(vendorName: string, shortlink: string): boolean {
			  const idx = mmIndex.get(vendorName)
			  if (idx === undefined) return false

			  const links = mmObj[idx].link

			// Max of 3 incidents per month to avoid giant array
			if (links.length >= 3) return false

			// check for dupes
			if (!links.includes(shortlink)) {
					links.push(shortlink)
					return true
			}

			return false
		}

        let changed = false

        // Atlassian API parser: fetch status page -> parse -> check incident -> append links
        async function apiAtl(vendorName: string, url: string) {
          const res = await fetch(url)
          if (!res.ok) {
            console.log(`Fetch failed for ${vendorName}: ${url} (status ${res.status})`)
            return
          }

          const parsed = (await res.json()) as apiAtlassian
          if (!parsed?.incidents?.length) return

          for (let i = 0; i < parsed.incidents.length; i++) {
            const impact = String(parsed.incidents[i].impact || "").toLowerCase()
            const shortlink = parsed.incidents[i].shortlink

            // Only use critical and major incidents
            if ((impact === "critical" || impact === "major" || impact === "minor") && typeof shortlink === "string" && shortlink.length > 0) { // MODIFIED: fixed corrupted impact check; only major/critical triggers
              changed = appendLink(vendorName, shortlink) || changed
            }
          }
        }

//###############################################################################
//                        RSS PART
//###############################################################################
        function startOfMonthUTC(): Date {
          const now = new Date()
          return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1))
        }

        async function hashString(input: string): Promise<string> {
          const data = new TextEncoder().encode(input)
          const digest = await crypto.subtle.digest("SHA-256", data)
          return btoa(String.fromCharCode(...new Uint8Array(digest)))
        }

        const SEEN_TTL_SECONDS = 60 * 60 * 24 * 90 // 90 days

                // RSS parser: fetch RSS status page -> parse -> check incident -> append links
        async function rss(vendorName: string, url: string) {
          const res = await fetch(url)
          if (!res.ok) return

          const feed = parseFeed(await res.text())
          const monthStart = startOfMonthUTC()

          // MODIFIED: gather eligible items first
          const items: { itemUrl: string; title: string; publishedMs: number }[] = [] // MODIFIED

          for (const item of feed.items ?? []) {
            if (!item.published) continue
            if (item.published < monthStart) continue

            const itemUrl = item.url?.trim() // MODIFIED: avoid shadowing param
            const title = item.title?.trim()
            if (!itemUrl || !title) continue

            const text = (title + " " + (item.description ?? "")).toLowerCase()
            if (text.includes("resolved") || text.includes("restored")) continue

            items.push({ itemUrl, title, publishedMs: item.published.getTime() }) // MODIFIED
          }

          if (!items.length) return // MODIFIED

          // MODIFIED: shuffle so "first unseen" is effectively random
          for (let i = items.length - 1; i > 0; i--) { // MODIFIED
            const j = Math.floor(Math.random() * (i + 1)) // MODIFIED
            const tmp = items[i]
            items[i] = items[j]
            items[j] = tmp
          }

          // MODIFIED: pick first unseen, but mark all as seen so none are picked again
          let pickedUrl: string | null = null // MODIFIED

          for (const it of items) {
            const keyInput = `${it.itemUrl}|${it.title}|${it.publishedMs}`
            const keyHash = await hashString(keyInput)
            const kvKey = `seen:rss:${keyHash}`

            const alreadySeen = await env.outage_bingo_kv.get(kvKey)
            if (!alreadySeen && pickedUrl === null) {
              pickedUrl = it.itemUrl // MODIFIED: this is the ONE we'll add
            }

            // MODIFIED: mark as seen regardless (prevents future selection)
            if (!alreadySeen) {
              await env.outage_bingo_kv.put(kvKey, "1", { expirationTtl: SEEN_TTL_SECONDS }) // MODIFIED
            }
          }

          if (!pickedUrl) return // MODIFIED: everything was already seen
          changed = appendLink(vendorName, pickedUrl) || changed // MODIFIED
        }



// Iterate outage-sources.json and call apiAtl for each Atlassian source or rss for RSS sources boilerplate code left in for other possible handlers
        for (let i = 0; i < sourcesObj.vendors.length; i++) {
          const vendor = sourcesObj.vendors[i]
          const vendorName = vendor.vendor

          for (let j = 0; j < vendor.sources.length; j++) {
            const source = vendor.sources[j]
            const type = source.type

            switch (type) {
              case "api_atlassian": {
                for (let k = 0; k < source.urls.length; k++) {
                  const url = source.urls[k]
                  await apiAtl(vendorName, url)
                }
                break
              }
              case "api_custom":{
                break
              }
              case "rss_hourly":{
                for (let k = 0; k < source.urls.length; k++){
                  const url = source.urls[k]
                  await rss(vendorName, url)
                }
                break
              }
              case "gdelt_daily":{
                break
              }
              default:
                // I guess we here now
                break
            }
          }
        }

// Write updated monthly JSON back to R2 once and only if something changed.
        changed && (await env.BINGO_BUCKET.put(mmKey, JSON.stringify(mmObj, null, 2), {httpMetadata: { contentType: "application/json" },}))


        break
      }
      case "0 */4 * * *": {

        // Load outage-sources.json
        const sourceTemplate = await env.BINGO_BUCKET.get("outage-sources.json")
        if (!sourceTemplate) throw new Error("Missing outage-sources.json")
        const sourcesText = await sourceTemplate.text()
        const sourcesObj = JSON.parse(sourcesText) as any


        // Load monthly outages-YYYY-MM.json into memory
        const mmKey = buildKey()
        const mmTemplate = await env.BINGO_BUCKET.get(mmKey)
        if (!mmTemplate) throw new Error(`Missing ${mmKey} in R2 (monthly file not created yet)`)
        const mmData = await mmTemplate.text()
        const mmObj = JSON.parse(mmData) as MonthlyOutageBingo[]

        // Map vendor name to index in mmObj so updates target the correct vendor (prevents overwriting all vendors)
        const mmIndex = new Map<string, number>()
        for (let i = 0; i < mmObj.length; i++) mmIndex.set(mmObj[i].name, i)

        // Append shortlink if vendor exists and link isn't already present
        function appendLink(vendorName: string, shortlink: string): boolean {
          const idx = mmIndex.get(vendorName)
          if (idx === undefined) return false

          const links = mmObj[idx].link

          // Max of 3 incidents per month to avoid giant array
          if (links.length >= 3) return false

          // check for dupes
          if (!links.includes(shortlink)) {
              links.push(shortlink)
              return true
          }

          return false
        }

        let changed = false

        function startOfMonthUTC(): Date {
          const now = new Date()
          return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1))
        }


        async function hashString(input: string): Promise<string> {
          const data = new TextEncoder().encode(input)
          const digest = await crypto.subtle.digest("SHA-256", data)
          return btoa(String.fromCharCode(...new Uint8Array(digest)))
        }

        const SEEN_TTL_SECONDS = 60 * 60 * 24 * 90 // 90 days


// RSS parser for category cells: fetch RSS -> parse -> pick 1 random unseen -> mark all unseen seen -> append link.
// Capped to 6 URLs per category (across ALL vendors/sources inside that category).
        async function rssCategory(categoryName: string, sourceVendor: string, url: string, cap: { added: number; max: number },) {
          if (cap.added >= cap.max) return

          const res = await fetch(url)
          if (!res.ok) return

          const feed = parseFeed(await res.text())
          const monthStart = startOfMonthUTC()

          // Collect unseen, eligible items first (do not write KV yet)
          const candidates: Array<{ itemUrl: string; kvKey: string }> = []

          for (const item of feed.items ?? []) {
            if (!item.published) continue
            if (item.published < monthStart) continue

            const itemUrl = item.url?.trim()
            const title = item.title?.trim()
            if (!itemUrl || !title) continue

            const EXCLUDED_TERMS = [
              "resolved",
              "restored",
              "postmortem",
              "review",
              "2025",
              "shababeek.org",
              "MSN",
              "stock",
              "acquired",
              "Class Action",
              "Settlement",
              "maintenance",
              "planned maintenance",
              "scheduled maintenance",
              "status update",
              "investigation concluded",
            ]

            const text = (title + " " + (item.description ?? "")).toLowerCase()
            if (EXCLUDED_TERMS.some(term => text.includes(term))) continue

            // Scope "seen" to category + vendor + article
            const keyInput = `${categoryName}|${sourceVendor}|${itemUrl}|${title}`
            const keyHash = await hashString(keyInput)
            const kvKey = `seen:rss:category:${keyHash}`

            const alreadySeen = await env.outage_bingo_kv.get(kvKey)
            if (alreadySeen) continue

            candidates.push({ itemUrl, kvKey })
          }

          if (!candidates.length) return

          // Pick one random unseen item (no shuffle)
          const picked = candidates[Math.floor(Math.random() * candidates.length)]

          if (cap.added >= cap.max) return

          // Append to the CATEGORY cell
          const didAdd = appendLink(categoryName, picked.itemUrl)
          if (!didAdd) return

          // Mark all unseen candidates as seen only after a successful add
          for (const c of candidates) {
            await env.outage_bingo_kv.put(c.kvKey, "1", {
              expirationTtl: SEEN_TTL_SECONDS,
            })
          }

          cap.added++
          changed = true
        }


        // Iterate outage-sources.json categories and apply per-category RSS logic
        for (let i = 0; i < sourcesObj.categories.length; i++) { // MODIFIED: added category iteration
          const category = sourcesObj.categories[i]
          const categoryName = category.name

          // Cap each category cell to 6 URLs total across all vendors/sources in the category
          const cap = { added: 0, max: 6 } //IN THE WRONG SPOT

          for (let j = 0; j < category.vendors.length; j++) {
            if (cap.added >= cap.max) break
            const vendor = category.vendors[j]
            const vendorName = vendor.vendor


            for (let k = 0; k < vendor.sources.length; k++) {
              if (cap.added >= cap.max) break
              const source = vendor.sources[k]
              const type = source.type
              switch (type) {
                case "rss_hourly": {
                  for (let u = 0; u < source.urls.length; u++) {
                    if (cap.added >= cap.max) break
                    const url = source.urls[u]
                    await rssCategory(categoryName, vendorName, url, cap)
                  }
                  break
                }

                default:
                  break
              }
            }
          }
        }

// Write updated monthly JSON back to R2 once and only if something changed.
        changed && (await env.BINGO_BUCKET.put(mmKey, JSON.stringify(mmObj, null, 2), {httpMetadata: { contentType: "application/json" },}))

        break
      }
    }
  },
} satisfies ExportedHandler<Env>
