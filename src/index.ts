import { connect } from "cloudflare:sockets";

interface Env {
	BINGO_BUCKET: R2Bucket;
}

function padMM(mm:number) {
	return String(mm).padStart(2, "0")
}




export default {
	// The scheduled handler is invoked at the interval set in our wrangler.jsonc's
	// [[triggers]] configuration.
	async scheduled(controller, env, ctx): Promise<void> {

		switch (controller.cron) {

			case "0,30 * 1 * *":
				//Build monthly bingo card
				const date = new Date()
				const yyyy = date.getUTCFullYear()
				const mm = padMM(date.getUTCMonth() + 1)
				const key = `outages-${yyyy}-${mm}.json`
				
				if(!await env.BINGO_BUCKET.head(key)) {
				const template =  await env.BINGO_BUCKET.get(`outages-template.json`)
				if(!template) throw new Error(`Missing Key`)
				await env.BINGO_BUCKET.put(key, template.body, {httpMetadata: { contentType: "application/json" }})
				}


				break;
			case  "* 1 * * *":
				// Hourly API check

				break;
		}

	}, // async scheduled
} satisfies ExportedHandler<Env>; // export default
