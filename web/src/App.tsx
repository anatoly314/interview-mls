import { JobsTable } from "./components/JobsTable";
import { UploadZone } from "./components/UploadZone";
import { useJobs } from "./useJobs";

export default function App() {
  const { jobs, connection, loadError, refresh, addOptimistic } = useJobs();

  return (
    <div className="page">
      <header className="header">
        <div>
          <h1>PDF Transaction Parser</h1>
          <p className="subtitle">
            Upload a bank statement; a worker extracts the transactions and returns a CSV.
          </p>
        </div>
        <span className={`connection connection-${connection}`}>
          <span className="dot" aria-hidden="true" />
          {connection === "live" ? "live" : "reconnecting"}
        </span>
      </header>

      <main>
        <UploadZone onAccepted={addOptimistic} />

        <section className="jobs-section">
          <h2>Jobs</h2>
          {loadError && (
            <p className="upload-error" role="alert">
              {loadError}
            </p>
          )}
          <JobsTable jobs={jobs} onRefresh={refresh} />
        </section>
      </main>
    </div>
  );
}
