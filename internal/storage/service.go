package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type FileMetadata struct {
	RealNodeID string `json:"realNodeId,omitempty"`
	FileName   string `json:"fileName"`
	Directory  string `json:"directory,omitempty"`
	Size       int64  `json:"size"`
	Status     string `json:"status"`
	Timestamp  int64  `json:"timestamp"`
}

type FileItem struct {
	Name      string    `json:"name"`
	Size      int64     `json:"size"`
	Directory bool      `json:"directory"`
	Timestamp time.Time `json:"timestamp"`
	NodeID    string    `json:"nodeId"`
}

type UploadResult struct {
	Success     bool
	Message     string
	FileDetails struct {
		Name   string `json:"name"`
		Size   int64  `json:"size"`
		Path   string `json:"path"`
		NodeID string `json:"nodeId"`
		FsID   int64  `json:"fsId"`
	}
}

type StorageService struct {
	cfg             *ServiceConfig
	client          *azblob.Client
	redis           *redis.Client
	storageDir      string
	accountName     string
	accountKey      string
	containerName   string
	metaCache       sync.Map // in-memory fast cache: map[string]*FileMetadata
	sfg             singleflight.Group
}

type ServiceConfig struct {
	ConnectionString string
	ContainerName    string
	StorageDir       string
	RedisHost        string
	RedisPort        int
	RedisPassword    string
	RedisDB          int
}

func HashNodeID(nodeID string) int64 {
	if nodeID == "" {
		return 0
	}
	var hash int32 = 0
	for _, b := range []byte(nodeID) {
		hash = ((hash << 5) - hash) + int32(b)
	}
	if hash < 0 {
		hash = -hash
	}
	return int64(hash)
}

func parseConnectionString(connStr string) (accountName, accountKey string, err error) {
	parts := strings.Split(connStr, ";")
	for _, p := range parts {
		if strings.HasPrefix(p, "AccountName=") {
			accountName = strings.TrimPrefix(p, "AccountName=")
		} else if strings.HasPrefix(p, "AccountKey=") {
			accountKey = strings.TrimPrefix(p, "AccountKey=")
		}
	}
	if accountName == "" || accountKey == "" {
		return "", "", fmt.Errorf("invalid Azure connection string: missing AccountName or AccountKey")
	}
	return accountName, accountKey, nil
}

func NewStorageService(cfg *ServiceConfig) (*StorageService, error) {
	accName, accKey, err := parseConnectionString(cfg.ConnectionString)
	if err != nil {
		return nil, err
	}

	client, err := azblob.NewClientFromConnectionString(cfg.ConnectionString, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create azure blob client: %w", err)
	}

	rClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort),
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	if err := os.MkdirAll(cfg.StorageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory %s: %w", cfg.StorageDir, err)
	}

	svc := &StorageService{
		cfg:           cfg,
		client:        client,
		redis:         rClient,
		storageDir:    cfg.StorageDir,
		accountName:   accName,
		accountKey:    accKey,
		containerName: cfg.ContainerName,
	}

	// Warm in-memory cache from local disk metadata if present
	svc.loadLocalMetadata()

	return svc, nil
}

func (s *StorageService) loadLocalMetadata() {
	metaFile := filepath.Join(s.storageDir, "metadata.json")
	data, err := os.ReadFile(metaFile)
	if err == nil {
		var m map[string]FileMetadata
		if err := json.Unmarshal(data, &m); err == nil {
			for k, v := range m {
				val := v
				s.metaCache.Store(k, &val)
			}
		}
	}
}

func (s *StorageService) ExtractIDFromURL(raw string) string {
	if raw == "" {
		return ""
	}
	if strings.Contains(raw, "/") {
		parts := strings.Split(raw, "/")
		return parts[len(parts)-1]
	}
	return raw
}

func (s *StorageService) GetMetadata(ctx context.Context, id string) *FileMetadata {
	// 1. L1 In-Memory cache (<0.01ms)
	if val, ok := s.metaCache.Load(id); ok {
		return val.(*FileMetadata)
	}

	// 2. L2 Redis
	val, err := s.redis.Get(ctx, "storage:metadata:"+id).Result()
	if err == nil && val != "" {
		var meta FileMetadata
		if err := json.Unmarshal([]byte(val), &meta); err == nil {
			s.metaCache.Store(id, &meta)
			return &meta
		}
	}
	return nil
}

func (s *StorageService) SetMetadata(ctx context.Context, id string, meta *FileMetadata) {
	s.metaCache.Store(id, meta)
	data, err := json.Marshal(meta)
	if err == nil {
		_ = s.redis.Set(ctx, "storage:metadata:"+id, string(data), 0).Err()
	}
}

func (s *StorageService) DeleteMetadata(ctx context.Context, id string) {
	s.metaCache.Delete(id)
	_ = s.redis.Del(ctx, "storage:metadata:"+id).Err()
}

// ResolveBlobName resolves temporary fsIds or requested IDs to the actual Azure blob name
func (s *StorageService) ResolveBlobName(ctx context.Context, fileID string) (blobName string, meta *FileMetadata, err error) {
	id := s.ExtractIDFromURL(fileID)

	// 1. Check if blob directly exists in Azure
	_, err = s.client.ServiceClient().NewContainerClient(s.containerName).NewBlobClient(id).GetProperties(ctx, nil)
	if err == nil {
		return id, s.GetMetadata(ctx, id), nil
	}

	// 2. Check metadata for realNodeId mapping
	m := s.GetMetadata(ctx, id)
	if m != nil && m.RealNodeID != "" {
		realHash := strconv.FormatInt(HashNodeID(m.RealNodeID), 10)
		_, err = s.client.ServiceClient().NewContainerClient(s.containerName).NewBlobClient(realHash).GetProperties(ctx, nil)
		if err == nil {
			return realHash, m, nil
		}
	}

	return id, m, fmt.Errorf("file not found: %s", fileID)
}

// EnsureFileCached ensures the file is cached locally on SSD and returns the absolute local path
func (s *StorageService) EnsureFileCached(ctx context.Context, fileID string) (localPath, filename, contentType string, err error) {
	id := s.ExtractIDFromURL(fileID)

	// Use Singleflight to deduplicate simultaneous requests for the same uncached file
	res, err, _ := s.sfg.Do(id, func() (interface{}, error) {
		blobName, meta, rErr := s.ResolveBlobName(ctx, id)
		if rErr != nil {
			return nil, rErr
		}

		targetLocalPath := filepath.Join(s.storageDir, blobName)
		fName := "file"
		if meta != nil && meta.FileName != "" {
			fName = meta.FileName
		}

		// Download from Azure to local SSD if not yet present
		if _, statErr := os.Stat(targetLocalPath); os.IsNotExist(statErr) {
			blobClient := s.client.ServiceClient().NewContainerClient(s.containerName).NewBlockBlobClient(blobName)
			
			resp, dlErr := blobClient.DownloadStream(ctx, nil)
			if dlErr != nil {
				return nil, fmt.Errorf("failed to download blob %s: %w", blobName, dlErr)
			}
			defer resp.Body.Close()

			tmpLocal := targetLocalPath + ".tmp." + strconv.FormatInt(time.Now().UnixNano(), 10)
			out, cErr := os.Create(tmpLocal)
			if cErr != nil {
				return nil, fmt.Errorf("failed to create local file %s: %w", tmpLocal, cErr)
			}

			if _, copyErr := io.Copy(out, resp.Body); copyErr != nil {
				out.Close()
				os.Remove(tmpLocal)
				return nil, fmt.Errorf("failed to stream blob to disk: %w", copyErr)
			}
			out.Close()

			if renErr := os.Rename(tmpLocal, targetLocalPath); renErr != nil {
				return nil, fmt.Errorf("failed to commit cached file: %w", renErr)
			}

			// Read Azure metadata if local metadata wasn't available
			if meta == nil {
				props, pErr := blobClient.GetProperties(ctx, nil)
				if pErr == nil {
					f := "file"
					if props.Metadata != nil {
						if fn, ok := props.Metadata["filename"]; ok && fn != nil {
							if decoded, dErr := url.QueryUnescape(*fn); dErr == nil {
								f = decoded
							}
						}
					}
					var size int64 = 0
					if props.ContentLength != nil {
						size = *props.ContentLength
					}
					meta = &FileMetadata{
						FileName:  f,
						Size:      size,
						Status:    "uploaded",
						Timestamp: time.Now().UnixMilli(),
					}
					s.SetMetadata(ctx, blobName, meta)
					if id != blobName {
						s.SetMetadata(ctx, id, meta)
					}
					fName = f
				}
			}
		}

		cType := mime.TypeByExtension(filepath.Ext(fName))
		if cType == "" {
			cType = "application/octet-stream"
		}

		return struct {
			localPath   string
			filename    string
			contentType string
		}{
			localPath:   targetLocalPath,
			filename:    fName,
			contentType: cType,
		}, nil
	})

	if err != nil {
		return "", "", "", err
	}

	outStruct := res.(struct {
		localPath   string
		filename    string
		contentType string
	})

	return outStruct.localPath, outStruct.filename, outStruct.contentType, nil
}

// UploadFile handles file upload directly to Azure Blob and stores metadata
func (s *StorageService) UploadFile(ctx context.Context, localFilePath, directory, oldPath string) (*UploadResult, error) {
	uploadPath, fileName, fileSize, compressed, err := PrepareImage(localFilePath)
	if err != nil {
		return nil, err
	}
	if compressed && uploadPath != localFilePath {
		defer os.Remove(uploadPath)
	}

	tempNodeID := fmt.Sprintf("temp_%d_%s", time.Now().UnixNano(), fileName)
	tempFsID := HashNodeID(tempNodeID)
	blobName := strconv.FormatInt(tempFsID, 10)

	// Save to local cache
	targetLocalPath := filepath.Join(s.storageDir, blobName)
	if err := copyFile(uploadPath, targetLocalPath); err != nil {
		return nil, fmt.Errorf("failed to cache local upload: %w", err)
	}

	// Open file for Azure Blob Upload
	f, err := os.Open(targetLocalPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open cached file for upload: %w", err)
	}
	defer f.Close()

	cType := mime.TypeByExtension(filepath.Ext(fileName))
	if cType == "" {
		cType = "application/octet-stream"
	}

	blobClient := s.client.ServiceClient().NewContainerClient(s.containerName).NewBlockBlobClient(blobName)
	_, err = blobClient.UploadStream(ctx, f, &azblob.UploadStreamOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: &cType,
		},
		Metadata: map[string]*string{
			"filename":  ptr(url.QueryEscape(fileName)),
			"directory": ptr(url.QueryEscape(directory)),
			"hashid":    ptr(blobName),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("azure blob upload failed: %w", err)
	}

	meta := &FileMetadata{
		FileName:  fileName,
		Directory: directory,
		Size:      fileSize,
		Status:    "uploaded",
		Timestamp: time.Now().UnixMilli(),
	}
	s.SetMetadata(ctx, blobName, meta)

	// Delete old file if provided
	if oldPath != "" {
		oldID := s.ExtractIDFromURL(oldPath)
		_ = s.DeleteFiles(ctx, []string{oldID})
	}

	res := &UploadResult{
		Success: true,
		Message: "File uploaded successfully",
	}
	res.FileDetails.Name = fileName
	res.FileDetails.Size = fileSize
	res.FileDetails.Path = filepath.Join(directory, fileName)
	res.FileDetails.NodeID = tempNodeID
	res.FileDetails.FsID = tempFsID

	return res, nil
}

// GenerateDownloadURL generates a secure 1-hour Azure SAS download URL
func (s *StorageService) GenerateDownloadURL(ctx context.Context, fileID string) (string, error) {
	blobName, _, err := s.ResolveBlobName(ctx, fileID)
	if err != nil {
		return "", err
	}

	cred, err := service.NewSharedKeyCredential(s.accountName, s.accountKey)
	if err != nil {
		return "", fmt.Errorf("failed to create SAS credential: %w", err)
	}

	now := time.Now().UTC()
	expiry := now.Add(1 * time.Hour)

	sasValues := sas.BlobPermissions{
		Read: true,
	}

	sasQueryParams, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     now,
		ExpiryTime:    expiry,
		Permissions:   sasValues.String(),
		ContainerName: s.containerName,
		BlobName:      blobName,
	}.SignWithSharedKey(cred)

	if err != nil {
		return "", fmt.Errorf("failed to sign SAS token: %w", err)
	}

	blobURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s?%s",
		s.accountName, s.containerName, blobName, sasQueryParams.Encode())

	return blobURL, nil
}

// DeleteFiles removes files from Azure Blob, local cache, and Redis metadata
func (s *StorageService) DeleteFiles(ctx context.Context, paths []string) error {
	for _, p := range paths {
		id := s.ExtractIDFromURL(p)
		targetLocalPath := filepath.Join(s.storageDir, id)
		_ = os.Remove(targetLocalPath)
		_ = os.Remove(targetLocalPath + ".png")

		blobClient := s.client.ServiceClient().NewContainerClient(s.containerName).NewBlobClient(id)
		_, _ = blobClient.Delete(ctx, nil)

		s.DeleteMetadata(ctx, id)
	}
	return nil
}

// FetchFileList lists all blobs in a virtual directory
func (s *StorageService) FetchFileList(ctx context.Context, directory string) ([]FileItem, error) {
	if directory == "" {
		directory = "/"
	}

	var items []FileItem
	containerClient := s.client.ServiceClient().NewContainerClient(s.containerName)
	pager := containerClient.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Metadata: true},
	})

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list blobs: %w", err)
		}
		for _, b := range resp.Segment.BlobItems {
			if *b.Name == "manifest.json" {
				continue
			}
			blobDir := "/"
			fName := *b.Name
			if b.Metadata != nil {
				if d, ok := b.Metadata["directory"]; ok && d != nil {
					if dec, err := url.QueryUnescape(*d); err == nil {
						blobDir = dec
					}
				}
				if f, ok := b.Metadata["filename"]; ok && f != nil {
					if dec, err := url.QueryUnescape(*f); err == nil {
						fName = dec
					}
				}
			}

			if directory == "/" || blobDir == directory {
				var size int64 = 0
				if b.Properties.ContentLength != nil {
					size = *b.Properties.ContentLength
				}
				var lm time.Time
				if b.Properties.LastModified != nil {
					lm = *b.Properties.LastModified
				}
				items = append(items, FileItem{
					Name:      fName,
					Size:      size,
					Directory: false,
					Timestamp: lm,
					NodeID:    *b.Name,
				})
			}
		}
	}

	return items, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func ptr[T any](v T) *T {
	return &v
}
