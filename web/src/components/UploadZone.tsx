import { useRef, useState } from "react";
import { uploadJob } from "../api";
import type { Accepted } from "../types";

interface Props {
  onAccepted: (job: Accepted) => void;
}

/** File picker plus a drop target. Uploads run one at a time -- the queue is
 *  server-side anyway, so serialising here costs nothing and keeps the error
 *  surface to a single message. */
export function UploadZone({ onAccepted }: Props) {
  const input = useRef<HTMLInputElement>(null);
  const [busy, setBusy] = useState(false);
  const [dragging, setDragging] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function send(files: FileList | File[]) {
    const pdfs = Array.from(files);
    if (pdfs.length === 0) return;
    setBusy(true);
    setError(null);
    try {
      for (const file of pdfs) {
        onAccepted(await uploadJob(file));
      }
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
      if (input.current) input.current.value = "";
    }
  }

  return (
    <section>
      <div
        className={`dropzone${dragging ? " dropzone-active" : ""}${busy ? " dropzone-busy" : ""}`}
        onDragOver={(e) => {
          e.preventDefault();
          if (!busy) setDragging(true);
        }}
        onDragLeave={() => setDragging(false)}
        onDrop={(e) => {
          e.preventDefault();
          setDragging(false);
          if (!busy) void send(e.dataTransfer.files);
        }}
      >
        <p className="dropzone-title">
          {busy ? "Uploading…" : "Drop a PDF statement here"}
        </p>
        <p className="dropzone-hint">PDF only, up to 32 MB</p>
        <button
          type="button"
          className="button button-primary"
          disabled={busy}
          onClick={() => input.current?.click()}
        >
          Choose file
        </button>
        <input
          ref={input}
          type="file"
          accept="application/pdf,.pdf"
          multiple
          hidden
          onChange={(e) => e.target.files && void send(e.target.files)}
        />
      </div>
      {error && (
        <p className="upload-error" role="alert">
          {error}
        </p>
      )}
    </section>
  );
}
