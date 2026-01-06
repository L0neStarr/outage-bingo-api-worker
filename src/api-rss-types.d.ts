// src/api-types.d.ts

type AtlassianIncident = {
  status: string;
  impact: string;
  shortlink: string
}

type apiAtlassian = {
  incidents: AtlassianIncident[]
}

type MonthlyOutageBingo = { 
name: string; 
link: string[] 
}