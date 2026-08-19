import { useEffect, useState } from "react";
import { cancelJob, downloadURL } from "../api";
import { relativeTime } from "../relative";
import type { Job } from "../types";
import { StatusBadge } from "./StatusBadge";

interface Props {
  jobs: Job[];
  onRefresh: () => void;
}

const CANCELABLE = new Set(["queued", "processing"]);

export function JobsTable({ jobs, onRefresh }: Props) {
  // Relative timestamps are the only thing that ages, so a slow tick is enough.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 15000);
    return () => clearInterval(id);
  }, []);

  const [canceling, setCanceling] = useState<Record<string, boolean>>({});
  const [notes, setNotes] = useState<Record<string, string>>({});

  async function cancel(id: string) {
    setCanceling((prev) => ({ ...prev, [id]: true }));
    setNotes((prev) => ({ ...prev, [id]: "" }));
    try {
      await cancelJob(id);
    } catch (err) {
      // 409 usually means the job finished while the button was on screen;
      // the refresh below makes the row agree with the server either way.
      setNotes((prev) => ({ ...prev, [id]: (err as Error).message }));
    } finally {
      setCanceling((prev) => ({ ...prev, [id]: false }));
      onRefresh();
    }
  }

  if (jobs.length === 0) {
    return <p className="empty">No jobs yet. Upload a statement to get started.</p>;
  }

  return (
    <table className="jobs">
      <thead>
        <tr>
          <th>File</th>
          <th>Status</th>
          <th>Created</th>
          <th className="col-actions">Actions</th>
        </tr>
      </thead>
      <tbody>
        {jobs.map((job) => (
          <tr key={job.id}>
            <td>
              <span className="filename" title={job.filename}>
                {job.filename}
              </span>
            </td>
            <td>
              <div className="status-cell">
                <StatusBadge status={job.status} />
                {job.attempt > 1 && (
                  <span className="attempt" title="processing attempts">
                    attempt {job.attempt}
                  </span>
                )}
              </div>
              {job.status === "failed" && job.error && (
                <p className="row-error" title={job.error}>
                  {job.error}
                </p>
              )}
              {notes[job.id] && <p className="row-note">{notes[job.id]}</p>}
            </td>
            <td className="muted" title={new Date(job.created_at).toLocaleString()}>
              {relativeTime(job.created_at, now)}
            </td>
            <td className="col-actions">
              <div className="actions">
                {job.has_csv && (
                  <a className="button" href={downloadURL(job.id)}>
                    Download
                  </a>
                )}
                {CANCELABLE.has(job.status) && (
                  <button
                    type="button"
                    className="button"
                    disabled={canceling[job.id]}
                    onClick={() => void cancel(job.id)}
                  >
                    Cancel
                  </button>
                )}
              </div>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
