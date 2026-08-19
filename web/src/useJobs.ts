import { useCallback, useEffect, useRef, useState } from "react";
import { listJobs, websocketURL } from "./api";
import type { Accepted, Job, JobEvent } from "./types";

export type Connection = "live" | "reconnecting";

const BACKOFF_MIN = 1000;
const BACKOFF_MAX = 10000;

function byNewest(a: Job, b: Job) {
  return b.created_at.localeCompare(a.created_at);
}

/** Owns the job list and the websocket.
 *
 *  The contract: the socket is a hint, GET /jobs is the truth. Events only ever
 *  patch a row that is already present -- they carry no filename, so a row can
 *  never be born from one -- and every (re)connect triggers a refetch that
 *  closes whatever gap the disconnect opened. */
export function useJobs() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [connection, setConnection] = useState<Connection>("reconnecting");
  const [loadError, setLoadError] = useState<string | null>(null);

  // Refetches are fire-and-forget from event handlers, so the newest response
  // wins by sequence number rather than by arrival order.
  const seq = useRef(0);

  const refresh = useCallback(async () => {
    const mine = ++seq.current;
    try {
      const fetched = await listJobs();
      if (mine !== seq.current) return;
      setJobs((prev) => {
        const known = new Set(fetched.map((j) => j.id));
        // Optimistic rows the server hasn't listed yet (a GET that raced an
        // upload) survive; the next refresh folds them in.
        const pending = prev.filter((j) => !known.has(j.id));
        return [...fetched, ...pending].sort(byNewest);
      });
      setLoadError(null);
    } catch (err) {
      if (mine === seq.current) setLoadError((err as Error).message);
    }
  }, []);

  /** Insert the 202 body from an upload. It is the authoritative queued state;
   *  ws and the next refetch take it from here. */
  const addOptimistic = useCallback((accepted: Accepted) => {
    const row: Job = {
      ...accepted,
      error: null,
      attempt: 0,
      updated_at: accepted.created_at,
      has_csv: false,
    };
    setJobs((prev) => [row, ...prev.filter((j) => j.id !== row.id)].sort(byNewest));
  }, []);

  useEffect(() => {
    let socket: WebSocket | null = null;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let backoff = BACKOFF_MIN;
    let closed = false;

    const connect = () => {
      socket = new WebSocket(websocketURL());

      socket.onopen = () => {
        backoff = BACKOFF_MIN;
        setConnection("live");
        // Reconciliation point: anything missed while disconnected shows up here.
        void refresh();
      };

      socket.onmessage = (msg) => {
        let ev: JobEvent;
        try {
          ev = JSON.parse(msg.data as string);
        } catch {
          return;
        }
        let seen = false;
        setJobs((prev) =>
          prev.map((job) => {
            if (job.id !== ev.job_id) return job;
            seen = true;
            return {
              ...job,
              status: ev.status,
              error: ev.error ?? null,
              has_csv: ev.status === "done" ? true : job.has_csv,
              updated_at: new Date().toISOString(),
            };
          }),
        );
        // A job someone else uploaded. Don't invent a row -- ask the server.
        if (!seen) void refresh();
      };

      socket.onclose = () => {
        if (closed) return;
        setConnection("reconnecting");
        retry = setTimeout(connect, backoff);
        backoff = Math.min(backoff * 2, BACKOFF_MAX);
      };

      // onclose always follows onerror; leave the reconnect to it.
      socket.onerror = () => socket?.close();
    };

    void refresh();
    connect();

    return () => {
      closed = true;
      clearTimeout(retry);
      socket?.close();
    };
  }, [refresh]);

  return { jobs, connection, loadError, refresh, addOptimistic };
}
