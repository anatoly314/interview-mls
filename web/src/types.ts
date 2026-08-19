export type Status = "queued" | "processing" | "done" | "failed";

/** A row of GET /jobs. */
export interface Job {
  id: string;
  filename: string;
  status: Status;
  error: string | null;
  attempt: number;
  created_at: string;
  updated_at: string;
  has_csv: boolean;
}

/** The 202 body of POST /jobs -- an authoritative queued job, minus the fields
 *  that only make sense once a worker has touched it. */
export interface Accepted {
  id: string;
  filename: string;
  status: "queued";
  created_at: string;
}

/** A server push on GET /ws. Carries no filename, so it can only ever update a
 *  row the client already knows about. */
export interface JobEvent {
  job_id: string;
  status: Status;
  error?: string;
}
