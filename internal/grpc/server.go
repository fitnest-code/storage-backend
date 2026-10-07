package grpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"fitnest-storage-backend/internal/storage"
	pb "fitnest-storage-backend/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedStorageServiceServer
	svc     *storage.StorageService
	tempDir string
}

func NewServer(svc *storage.StorageService, tempDir string) *Server {
	_ = os.MkdirAll(tempDir, 0755)
	return &Server{
		svc:     svc,
		tempDir: tempDir,
	}
}

func (s *Server) UploadFile(stream pb.StorageService_UploadFileServer) error {
	var meta *pb.FileMetadata
	var buffer bytes.Buffer

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to read stream: %v", err)
		}

		if m := req.GetMetadata(); m != nil {
			meta = m
		}
		if chunk := req.GetChunkData(); len(chunk) > 0 {
			buffer.Write(chunk)
		}
	}

	if meta == nil {
		return status.Errorf(codes.InvalidArgument, "no file metadata received")
	}

	tempFile := filepath.Join(s.tempDir, fmt.Sprintf("grpc-%d-%s", time.Now().UnixNano(), meta.GetFilename()))
	if err := os.WriteFile(tempFile, buffer.Bytes(), 0644); err != nil {
		return status.Errorf(codes.Internal, "failed to write temp file: %v", err)
	}
	defer os.Remove(tempFile)

	dir := meta.GetDirectory()
	if dir == "" {
		dir = "/uploads"
	}

	ctx := stream.Context()
	result, err := s.svc.UploadFile(ctx, tempFile, dir, meta.GetOldPath())
	if err != nil {
		if errors.Is(err, storage.ErrFileTooLarge) || errors.Is(err, storage.ErrInvalidType) {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		return status.Errorf(codes.Internal, "upload failed: %v", err)
	}

	return stream.SendAndClose(&pb.UploadFileResponse{
		Success: true,
		Message: "File uploaded successfully",
		Data: &pb.StorageFileData{
			Path:  result.FileDetails.Path,
			Size:  result.FileDetails.Size,
			Md5:   "",
			FsId:  result.FileDetails.FsID,
		},
	})
}

func (s *Server) GetDownloadUrl(ctx context.Context, req *pb.GetDownloadUrlRequest) (*pb.GetDownloadUrlResponse, error) {
	fileID := req.GetFileId()
	if fileID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "file_id is required")
	}

	url, err := s.svc.GenerateDownloadURL(ctx, fileID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to generate download url: %v", err)
	}

	return &pb.GetDownloadUrlResponse{
		Success:     true,
		Message:     "Download URL retrieved",
		DownloadUrl: url,
	}, nil
}

func (s *Server) DownloadFile(req *pb.DownloadFileRequest, stream pb.StorageService_DownloadFileServer) error {
	fileID := req.GetFileId()
	if fileID == "" {
		return status.Errorf(codes.InvalidArgument, "file_id is required")
	}

	ctx := stream.Context()
	localPath, filename, contentType, err := s.svc.EnsureFileCached(ctx, fileID)
	if err != nil {
		if errors.Is(err, storage.ErrFileNotFound) {
			return status.Errorf(codes.NotFound, "file not found: %v", err)
		}
		// Transient storage failure — Unavailable tells callers to retry
		// instead of treating the content as permanently missing.
		return status.Errorf(codes.Unavailable, "storage unavailable: %v", err)
	}

	// Send metadata first
	if err := stream.Send(&pb.DownloadFileResponse{
		Response: &pb.DownloadFileResponse_Metadata{
			Metadata: &pb.FileMetadata{
				Filename:    filename,
				Directory:   "/",
				ContentType: contentType,
			},
		},
	}); err != nil {
		return status.Errorf(codes.Internal, "failed to send metadata: %v", err)
	}

	// Stream chunks from local cached file
	f, err := os.Open(localPath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to open local cached file: %v", err)
	}
	defer f.Close()

	buf := make([]byte, 64*1024) // 64KB chunks
	for {
		n, err := f.Read(buf)
		if n > 0 {
			if sErr := stream.Send(&pb.DownloadFileResponse{
				Response: &pb.DownloadFileResponse_FileData{
					FileData: buf[:n],
				},
			}); sErr != nil {
				return status.Errorf(codes.Internal, "failed to send chunk: %v", sErr)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to read file: %v", err)
		}
	}

	return nil
}

func (s *Server) DeleteFiles(ctx context.Context, req *pb.DeleteFilesRequest) (*pb.DeleteFilesResponse, error) {
	paths := req.GetPaths()
	if len(paths) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "paths list is empty")
	}

	if err := s.svc.DeleteFiles(ctx, paths); err != nil {
		return nil, status.Errorf(codes.Internal, "delete failed: %v", err)
	}

	return &pb.DeleteFilesResponse{
		Success: true,
		Message: "Paths deleted successfully",
	}, nil
}

func patternAUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		gateway := md.Get("x-from-gateway")
		userId := md.Get("x-user-id")
		caller := md.Get("x-service-name")
		if len(gateway) > 0 && gateway[0] == "1" && len(userId) > 0 {
			c := "unknown"
			if len(caller) > 0 {
				c = caller[0]
			}
			fmt.Printf("[gRPC] Authenticated user %s via Pattern A (from %s)\n", userId[0], c)
		}
	}
	return handler(ctx, req)
}

func StartGRPCServer(port int, svc *storage.StorageService, tempDir string) (*grpc.Server, net.Listener, error) {
	addr := ":" + strconv.Itoa(port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(patternAUnaryInterceptor),
	)

	serverImpl := NewServer(svc, tempDir)
	pb.RegisterStorageServiceServer(grpcServer, serverImpl)

	return grpcServer, lis, nil
}
