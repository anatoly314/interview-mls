import type { Accepted, Job } from "./types";

/** Every error response is {"error":"..."}; fall back to the status line if a
 *  proxy or the platform returned something else (413 can arrive as HTML). */
async function fail(res: Response): Promise<never> {
  let message = `request failed (${res.status})`;
  try {
    const body = await res.json();
    if (body && typeof body.error === "string") message = body.error;
  } catch {
    if (res.status === 413) message = "file too large (32MB max)";
  }
  throw new Error(message);
}

export async function listJobs(signal?: AbortSignal): Promise<Job[]> {
  const res = await fetch("/jobs", { signal });
  if (!res.ok) return fail(res);
  return res.json();
}

export async function uploadJob(file: File): Promise<Accepted> {
  const body = new FormData();
  body.append("file", file);
  const res = await fetch("/jobs", { method: "POST", body });
  if (!res.ok) return fail(res);
  return res.json();
}

export async function cancelJob(id: string): Promise<void> {
  const res = await fetch(`/jobs/${id}/cancel`, { method: "POST" });
  if (!res.ok) return fail(res);
}

export function downloadURL(id: string): string {
  return `/jobs/${id}/download`;
}

export function websocketURL(): string {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}/ws`;
}
