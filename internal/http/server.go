package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fitnest-storage-backend/internal/response"
	"fitnest-storage-backend/internal/storage"
)

type HTTPServer struct {
	svc     *storage.StorageService
	tempDir string
}

func NewHTTPServer(svc *storage.StorageService, tempDir string) *HTTPServer {
	_ = os.MkdirAll(tempDir, 0755)
	return &HTTPServer{
		svc:     svc,
		tempDir: tempDir,
	}
}

func (s *HTTPServer) SetupRoutes() http.Handler {
	mux := http.NewServeMux()

	// Health
	mux.HandleFunc("/health", s.handleHealth)

	// Media streaming (fast path)
	mux.HandleFunc("/api/v1/media/stream/", s.handleStream)
	mux.HandleFunc("/stream/", s.handleStream)

	// Files API
	mux.HandleFunc("/api/v1/files/upload", s.handleUpload)
	mux.HandleFunc("/api/v1/files/list", s.handleList)
	mux.HandleFunc("/api/v1/files/download", s.handleDownload)
	mux.HandleFunc("/api/v1/files/delete", s.handleDelete)
	mux.HandleFunc("/api/v1/files/move", s.handleMove)

	// Root wrapper with CORS and Pattern A middleware
	return s.corsMiddleware(s.patternAMiddleware(s.notFoundMiddleware(mux)))
}

func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "UP"})
}

func (s *HTTPServer) handleStream(w http.ResponseWriter, r *http.Request) {
	lang := response.GetLang(r)
	path := r.URL.Path

	parts := strings.Split(strings.TrimRight(path, "/"), "/")
	if len(parts) == 0 {
		response.Error(w, http.StatusBadRequest, "BAD_REQUEST", "error.bad_request", path, lang, nil)
		return
	}
	fileID := parts[len(parts)-1]

	ctx := r.Context()
	localPath, filename, _, err := s.svc.EnsureFileCached(ctx, fileID)
	if err != nil {
		fmt.Printf("[HTTPServer] Stream failed for fileId '%s' (url: %s): %v\n", fileID, path, err)
		if errors.Is(err, context.Canceled) {
			// Client disconnected mid-request; nothing meaningful to answer.
			return
		}
		if errors.Is(err, storage.ErrFileNotFound) {
			response.Error(w, http.StatusNotFound, "FILE_NOT_FOUND", "error.file_not_found", path, lang, nil)
			return
		}
		// Transient storage/Azure failure: 502 tells clients (and us, via logs)
		// to retry instead of caching a permanent "missing image".
		response.Error(w, http.StatusBadGateway, "STORAGE_UNAVAILABLE", "error.internal_server_error", path, lang, nil)
		return
	}

	// Set cache headers: 1 year immutable
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", url.QueryEscape(filename)))

	// http.ServeFile uses Linux zero-copy sendfile syscall for maximum I/O performance
	http.ServeFile(w, r, localPath)
}

func (s *HTTPServer) handleUpload(w http.ResponseWriter, r *http.Request) {
	lang := response.GetLang(r)
	path := r.URL.Path

	if r.Method != http.MethodPost {
		response.Error(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "error.bad_request", path, lang, nil)
		return
	}

	// Buffer at most 8MB in RAM; larger parts spill to temp files (the old
	// 100MB-in-RAM setting was OOMKilling the 384Mi pod on big uploads).
	if err := r.ParseMultipartForm(8 << 20); err != nil {
		response.Error(w, http.StatusBadRequest, "BAD_REQUEST", "error.bad_request", path, lang, nil)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "BAD_REQUEST", "error.no_file_uploaded", path, lang, nil)
		return
	}
	defer file.Close()

	directory := r.URL.Query().Get("directory")
	if directory == "" {
		directory = r.FormValue("directory")
	}
	uploadType := r.FormValue("type")
	finalDirectory := "/uploads"
	if uploadType == "profile" {
		finalDirectory = "/profiles"
	} else if uploadType == "goal" {
		finalDirectory = "/goals"
	} else if directory != "" {
		finalDirectory = directory
	}

	tempFilePath := filepath.Join(s.tempDir, fmt.Sprintf("http-%d-%s", time.Now().UnixNano(), header.Filename))
	tempDest, err := os.Create(tempFilePath)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "error.internal_server_error", path, lang, nil)
		return
	}
	if _, err := io.Copy(tempDest, file); err != nil {
		tempDest.Close()
		_ = os.Remove(tempFilePath)
		response.Error(w, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "error.internal_server_error", path, lang, nil)
		return
	}
	tempDest.Close()
	defer os.Remove(tempFilePath)

	ctx := r.Context()
	result, err := s.svc.UploadFile(ctx, tempFilePath, finalDirectory, "")
	if err != nil {
		fmt.Printf("[HTTPServer] Upload error: %v\n", err)
		if errors.Is(err, storage.ErrFileTooLarge) {
			response.Error(w, http.StatusBadRequest, "FILE_TOO_LARGE", "error.file_too_large", path, lang, nil)
			return
		}
		if errors.Is(err, storage.ErrInvalidType) {
			response.Error(w, http.StatusBadRequest, "INVALID_FILE_TYPE", "error.invalid_file_type", path, lang, nil)
			return
		}
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "error.upload_failed", path, lang, nil)
		return
	}

	response.Success(w, http.StatusOK, result.FileDetails)
}

func (s *HTTPServer) handleList(w http.ResponseWriter, r *http.Request) {
	lang := response.GetLang(r)
	path := r.URL.Path

	dir := r.URL.Query().Get("directory")
	if dir == "" {
		dir = "/"
	}

	items, err := s.svc.FetchFileList(r.Context(), dir)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "FETCH_FAILED", "error.fetch_failed", path, lang, nil)
		return
	}

	response.Success(w, http.StatusOK, items)
}

func (s *HTTPServer) handleDownload(w http.ResponseWriter, r *http.Request) {
	lang := response.GetLang(r)
	path := r.URL.Path

	fileID := r.URL.Query().Get("fileId")
	if fileID == "" {
		response.Error(w, http.StatusBadRequest, "BAD_REQUEST", "error.bad_request", path, lang, nil)
		return
	}

	url, err := s.svc.GenerateDownloadURL(r.Context(), fileID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "DOWNLOAD_FAILED", "error.unexpected", path, lang, nil)
		return
	}

	response.Success(w, http.StatusOK, map[string]string{
		"download_url": url,
	})
}

func (s *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	lang := response.GetLang(r)
	path := r.URL.Path

	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		response.Error(w, http.StatusMethodNotAllowed, "BAD_REQUEST", "error.bad_request", path, lang, nil)
		return
	}

	var paths []string
	if err := json.NewDecoder(r.Body).Decode(&paths); err != nil {
		response.Error(w, http.StatusBadRequest, "BAD_REQUEST", "error.bad_request", path, lang, nil)
		return
	}

	if err := s.svc.DeleteFiles(r.Context(), paths); err != nil {
		response.Error(w, http.StatusInternalServerError, "DELETE_FAILED", "error.delete_failed", path, lang, nil)
		return
	}

	response.Success(w, http.StatusOK, nil)
}

type movePayload struct {
	OldPath string `json:"old_path"`
	NewPath string `json:"new_path"`
	NewName string `json:"new_name"`
}

func (s *HTTPServer) handleMove(w http.ResponseWriter, r *http.Request) {
	lang := response.GetLang(r)
	path := r.URL.Path

	var p movePayload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil || p.OldPath == "" || p.NewPath == "" {
		response.Error(w, http.StatusBadRequest, "BAD_REQUEST", "error.bad_request", path, lang, nil)
		return
	}

	// Update metadata
	ctx := r.Context()
	id := s.svc.ExtractIDFromURL(p.OldPath)
	meta := s.svc.GetMetadata(ctx, id)
	if meta != nil {
		meta.Directory = p.NewPath
		if p.NewName != "" {
			meta.FileName = p.NewName
		}
		s.svc.SetMetadata(ctx, id, meta)
	}

	response.Success(w, http.StatusOK, nil)
}

func (s *HTTPServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (s *HTTPServer) patternAMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatewayFlag := r.Header.Get("X-From-Gateway")
		userId := r.Header.Get("X-User-Id")
		caller := r.Header.Get("X-Service-Name")

		if gatewayFlag == "1" && userId != "" {
			fmt.Printf("[HTTP] Authenticated user %s via Pattern A (from %s)\n", userId, caller)
		}

		next.ServeHTTP(w, r)
	})
}

func (s *HTTPServer) notFoundMiddleware(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Custom handler wrapper if needed
		mux.ServeHTTP(w, r)
	})
}
