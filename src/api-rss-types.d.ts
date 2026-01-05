// src/api-types.d.ts

type apiAtlassian = {
  incidents: {
    status: string
    impact: string
    shortlink: string
  }[]
}

type monthlyOutageBingo = {
  name: string
  link: string[]
}