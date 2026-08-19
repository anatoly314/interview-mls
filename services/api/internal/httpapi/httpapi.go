// Package httpapi is the api's HTTP surface: upload, list, download, cancel,
// the websocket endpoint and the static UI.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"

	"github.com/anatolyt/interview-mls/services/api/internal/events"
	"github.com/anatolyt/interview-mls/services/api/internal/hub"
	"github.com/anatolyt/interview-mls/services/api/internal/store"
)

const (
	// maxUpload bounds the request body; a bank statement is kilobytes, so this
	// is generous and exists only to stop a client from filling the disk.
	maxUpload = 32 << 20
	// uploadField is the multipart field name the UI must use.
	uploadField = "file"
	// canceledError is the error text a canceled job carries. Cancel reuses
	// 'failed' rather than adding a status value -- the schema stays stable and
	// the UI already renders error text.
	canceledError = "canceled by user"
	// webDir is the built React UI, mounted in the image when it exists.
	webDir = "./web/dist"
)

type Handler struct {
	log    *slog.Logger
	store  *store.Store
	s3     *minio.Client
	bucket string
	hub    *hub.Hub
	events *events.Server
}

func New(log *slog.Logger, st *store.Store, s3 *minio.Client, bucket string,
	h *hub.Hub, ev *events.Server) *Handler {
	return &Handler{log: log, store: st, s3: s3, bucket: bucket, hub: h, events: ev}
}

func (h *Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /jobs", h.upload)
	mux.HandleFunc("GET /jobs", h.list)
	mux.HandleFunc("GET /jobs/{id}/download", h.download)
	mux.HandleFunc("POST /jobs/{id}/cancel", h.cancel)
	mux.Handle("GET /ws", h.hub)
	mux.Handle("GET /", h.ui())
	return mux
}

// upload streams the pdf to MinIO first and commits the row second.
//
// The order is the point. Dying between the two leaves an orphaned object with
// no row pointing at it -- garbage a lifecycle rule sweeps up. The reverse
// order would leave a claimable job whose source pdf doesn't exist, which is a
// correctness bug. Commit the pointer last.
func (h *Handler) upload(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxUpload)

	part, err := pdfPart(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer part.Close()

	id := uuid.NewString()
	filename := path.Base(filepath.ToSlash(part.FileName()))
	pdfKey := fmt.Sprintf("jobs/%s/source.pdf", id)

	// size -1: the multipart part has no length, so minio streams it.
	if _, err := h.s3.PutObject(r.Context(), h.bucket, pdfKey, part, -1,
		minio.PutObjectOptions{ContentType: "application/pdf"}); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "file too large")
			return
		}
		h.log.Error("put pdf", "key", pdfKey, "err", err)
		writeError(w, http.StatusInternalServerError, "could not store upload")
		return
	}

	createdAt, err := h.store.Enqueue(r.Context(), id, filename, pdfKey)
	if err != nil {
		h.log.Error("enqueue", "job_id", id, "err", err)
		// Best effort: the object is unreferenced either way, this just saves
		// the lifecycle rule the trouble.
		rmCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if rmErr := h.s3.RemoveObject(rmCtx, h.bucket, pdfKey, minio.RemoveObjectOptions{}); rmErr != nil {
			h.log.Warn("orphan pdf left behind", "key", pdfKey, "err", rmErr)
		}
		writeError(w, http.StatusInternalServerError, "could not enqueue job")
		return
	}

	h.log.Info("job enqueued", "job_id", id, "filename", filename)
	writeJSON(w, http.StatusAccepted, map[string]any{
		"id":         id,
		"filename":   filename,
		"status":     "queued",
		"created_at": createdAt,
	})
}

// pdfPart pulls the "file" part out of the multipart body without buffering it
// to memory or a temp file -- the body streams straight through to MinIO.
func pdfPart(r *http.Request) (*multipart.Part, error) {
	mr, err := r.MultipartReader()
	if err != nil {
		return nil, fmt.Errorf("expected a multipart upload")
	}
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("missing %q field", uploadField)
		}
		if err != nil {
			return nil, fmt.Errorf("malformed upload")
		}
		if part.FormName() != uploadField {
			part.Close()
			continue
		}
		if !strings.EqualFold(filepath.Ext(part.FileName()), ".pdf") {
			part.Close()
			return nil, fmt.Errorf("only .pdf files are accepted")
		}
		return part, nil
	}
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.store.List(r.Context())
	if err != nil {
		h.log.Error("list jobs", "err", err)
		writeError(w, http.StatusInternalServerError, "could not list jobs")
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (h *Handler) download(w http.ResponseWriter, r *http.Request) {
	job, err := h.lookup(w, r)
	if err != nil {
		return
	}
	if job.Status != "done" || job.CSVKey == nil {
		writeError(w, http.StatusNotFound, "no result for this job")
		return
	}

	obj, err := h.s3.GetObject(r.Context(), h.bucket, *job.CSVKey, minio.GetObjectOptions{})
	if err != nil {
		h.log.Error("get csv", "job_id", job.ID, "err", err)
		writeError(w, http.StatusInternalServerError, "could not read result")
		return
	}
	defer obj.Close()

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf("attachment; filename=%q", csvName(job.Filename)))
	if _, err := io.Copy(w, obj); err != nil {
		// Headers are already out; all that's left is a log line.
		h.log.Warn("stream csv", "job_id", job.ID, "err", err)
	}
}

// cancel takes the queued path or the routed path depending on where the job
// is. A queued job has no lease holder, so the api fails it directly; the
// UPDATE's status guard handles the race where a worker claims it in between,
// and we fall through to routing a command at the new lease holder.
func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	job, err := h.lookup(w, r)
	if err != nil {
		return
	}
	switch job.Status {
	case "done", "failed":
		writeError(w, http.StatusConflict, "job already finished")
		return
	case "queued":
		ok, err := h.store.CancelQueued(r.Context(), job.ID, canceledError)
		if err != nil {
			h.log.Error("cancel queued", "job_id", job.ID, "err", err)
			writeError(w, http.StatusInternalServerError, "could not cancel job")
			return
		}
		if ok {
			h.log.Info("queued job canceled", "job_id", job.ID)
			h.hub.Broadcast(hub.Event{JobID: job.ID, Status: "failed", Error: canceledError})
			writeJSON(w, http.StatusAccepted, map[string]any{"id": job.ID, "status": "failed"})
			return
		}
	}

	if !h.events.Cancel(job.ID) {
		writeError(w, http.StatusConflict, "cannot cancel right now")
		return
	}
	h.log.Info("cancel routed to lease holder", "job_id", job.ID)
	writeJSON(w, http.StatusAccepted, map[string]any{"id": job.ID, "status": "canceling"})
}

// lookup resolves the {id} path value, writing the error response itself so
// handlers can just return on a non-nil error.
func (h *Handler) lookup(w http.ResponseWriter, r *http.Request) (store.Detail, error) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusNotFound, "unknown job")
		return store.Detail{}, err
	}
	job, err := h.store.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "unknown job")
		return store.Detail{}, err
	}
	if err != nil {
		h.log.Error("get job", "job_id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "could not read job")
		return store.Detail{}, err
	}
	return job, nil
}

// ui serves the built React app when it's there, and otherwise a placeholder,
// so the api and /healthz work standalone.
func (h *Handler) ui() http.Handler {
	if _, err := os.Stat(filepath.Join(webDir, "index.html")); err == nil {
		return http.FileServer(http.Dir(webDir))
	}
	h.log.Info("no built UI found, serving placeholder", "dir", webDir)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, placeholder)
	})
}

const placeholder = `<!doctype html>
<meta charset="utf-8"><title>PDF Transaction Parser</title>
<body style="font:16px system-ui;max-width:40em;margin:4em auto">
<h1>PDF Transaction Parser</h1>
<p>The React UI lands in R3. The api is up: try
<code>POST /jobs</code>, <code>GET /jobs</code>, <code>GET /ws</code>.</p>
</body>`

func csvName(pdfName string) string {
	base := path.Base(filepath.ToSlash(pdfName))
	return strings.TrimSuffix(base, filepath.Ext(base)) + ".csv"
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
