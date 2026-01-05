// index.ts — copy/paste replacement
// Scope: (1) ensure monthly outages-YYYY-MM.json exists from template, (2) hourly parse Atlassian incident feeds,
// (3) append incident shortlinks into the correct vendor's link[] (no duplicates), (4) write the updated JSON back to R2 once.

interface Env {
  BINGO_BUCKET: R2Bucket
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

type MonthlyOutageBing = { name: string; link: string[] } // link is an array (matches your updated template)
type AtlassianIncident = { status: string; impact: string; shortlink: string }
type apiAtlassian = { incidents: AtlassianIncident[] }

export default {
  async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    switch (controller.cron) {
      case "0,30 * 1 * *": {
        // Build monthly template file (only if missing)
        const key = buildKey()

        if (!(await env.BINGO_BUCKET.head(key))) {
          const template = await env.BINGO_BUCKET.get("outages-template.json")
          if (!template) throw new Error("Missing outages-template.json")

          // Copies template bytes directly (efficient for initialization; no JSON parsing needed here)
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
        const mmObj = JSON.parse(mmData) as MonthlyOutageBing[]

        // Map vendor name -> index in mmObj so updates target the correct vendor (prevents overwriting all vendors)
        const mmIndex = new Map<string, number>()
        for (let i = 0; i < mmObj.length; i++) mmIndex.set(mmObj[i].name, i)

        // Append helper: adds shortlink if vendor exists and link isn't already present
        function appendLink(vendorName: string, shortlink: string): boolean {
          const idx = mmIndex.get(vendorName)
          if (idx === undefined) {
            console.log(`Vendor not found in ${mmKey}: "${vendorName}"`)
            return false
          }

          // Defensive in case older months have the wrong shape
          if (!Array.isArray(mmObj[idx].link)) mmObj[idx].link = []

          if (!mmObj[idx].link.includes(shortlink)) {
            mmObj[idx].link.push(shortlink)
            return true
          }

          return false
        }

        let changed = false

        // Atlassian API parser: fetch -> parse -> apply criteria -> append links
        async function apiAtl(vendorName: string, url: string) {
          const res = await fetch(url)
          if (!res.ok) {
            console.log(`Fetch failed for ${vendorName}: ${url} (status ${res.status})`)
            return
          }

          const parsed = (await res.json()) as apiAtlassian
          if (!parsed?.incidents?.length) return

          for (let i = 0; i < parsed.incidents.length; i++) {
            const impact = parsed.incidents[i].impact
            const shortlink = parsed.incidents[i].shortlink

            // Your current criterion: only critical incidents
            if ((impact === "critical" || impact === "major") && typeof shortlink === "string" && shortlink.length > 0) {
              changed = appendLink(vendorName, shortlink) || changed
            }
          }
        }

        // Iterate outage-sources.json and call apiAtl for each Atlassian source
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

              case "api_custom":
              case "rss_daily":
              case "gdelt_daily":
              default:
                // Not implemented here (per your scope); leave as no-op
                break
            }
          }
        }

        // Write updated monthly JSON back to R2 once (only if something changed)
        changed &&
          (await env.BINGO_BUCKET.put(mmKey, JSON.stringify(mmObj, null, 2), {
            httpMetadata: { contentType: "application/json" },
          }))

        console.log(mmObj)
        break
      }
    }
  },
} satisfies ExportedHandler<Env>
