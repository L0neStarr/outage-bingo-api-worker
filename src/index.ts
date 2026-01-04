import { connect } from "cloudflare:sockets";

interface Env {
	BINGO_BUCKET: R2Bucket;
}

function padMM(mm:number) {
	return String(mm).padStart(2, "0")
}




export default {

	async fetch(req) {

		const url = new URL(req.url);
		url.pathname = '/__scheduled';
		url.searchParams.append('cron', '* * * * *');
		return new Response(`To test the scheduled handler, ensure you have used the "--test-scheduled" then try running "curl ${url.href}".`);


	}, // async fetch
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
			case  "0 * * * *":
				// Hourly API check
				const apiTemplate	= await env.BINGO_BUCKET.get(`outage-sources.json`)
				if (!apiTemplate) throw new Error("Missing outage-sources.json")
				const data = await apiTemplate.text()
				const obj = JSON.parse(data)

				//Atlssian API parser
				async function apiAtl(source: string[]) {
					//logic
				}



				//console.log(sources.vendors[0].vendor) //this is i

				for (let i = 0; i < obj.vendors.length; i++) {
					var vendor = obj.vendors[i]
					//console.log(vendors)
					var vendorName = obj.vendors[i].vendor
					//console.log(vendors)
					//console.log(vendors.sources.length)

					for(let j = 0; j < vendor.sources.length; j++){
						var source = vendor.sources[j]
						var types = vendor.sources[j].type
						//console.log(types)
						for(let k = 0; k <source.urls.length; k++){
							var type = source.type
							var url = source.urls[k]
							//console.log(type)
						}
					}
				}

				

				// Cloudflare


				break;
		}

	}, // async scheduled
} satisfies ExportedHandler<Env>; // export default
