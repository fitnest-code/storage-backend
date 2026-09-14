package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"fitnest-storage-backend/internal/config"
	grpcServer "fitnest-storage-backend/internal/grpc"
	httpServer "fitnest-storage-backend/internal/http"
	"fitnest-storage-backend/internal/response"
	"fitnest-storage-backend/internal/storage"
	"fitnest-storage-backend/internal/vault"
)

func main() {
	fmt.Println("[Bootstrap] Starting Fitnest Storage Backend (Go)...")

	// 1. Vault Secret Loading
	vaultAddr := os.Getenv("VAULT_ADDR")
	roleID := os.Getenv("VAULT_ROLE_ID")
	secretID := os.Getenv("VAULT_SECRET_ID")

	if roleID != "" && secretID != "" {
		fmt.Printf("[Vault] Connecting to %s...\n", vaultAddr)
		vClient, err := vault.NewClient(vaultAddr)
		if err != nil {
			fmt.Printf("[Vault] Warning: Failed to create vault client: %v\n", err)
		} else {
			if err := vClient.LoginAppRole(roleID, secretID); err != nil {
				fmt.Printf("[Vault] Warning: AppRole login failed: %v\n", err)
			} else {
				fmt.Println("[Vault] Login successful.")
				secretsPath := os.Getenv("VAULT_SECRETS_PATH")
				if secretsPath == "" {
					secretsPath = "secrets/data/storage-backend/production"
				}
				fmt.Printf("[Vault] Reading secrets from %s...\n", secretsPath)
				secrets, err := vClient.LoadSecrets(secretsPath)
				if err != nil {
					fmt.Printf("[Vault] Warning: Failed to load secrets: %v\n", err)
				} else {
					fmt.Printf("[Vault] Successfully loaded %d secrets.\n", len(secrets))
				}
				// Start background auto-renewal
				vClient.StartAutoRenew(context.Background(), 10*time.Minute)
			}
		}
	} else {
		fmt.Println("[Vault] VAULT_ROLE_ID not provided, skipping Vault loading.")
	}

	// 2. Load Configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		fmt.Printf("[Config] Fatal error loading config: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Printf("[Config] Warning: Configuration incomplete: %v\n", err)
	}

	// Load localizations
	localesDir := filepath.Join("src", "locales")
	response.LoadLocales(localesDir)

	// 3. Initialize Storage Service
	fmt.Println("[StorageService] Initializing Azure Blob Storage & Redis cache...")
	svc, err := storage.NewStorageService(&storage.ServiceConfig{
		ConnectionString: cfg.AzureStorageConnectionString,
		ContainerName:    cfg.AzureStorageContainer,
		StorageDir:       cfg.StorageDir,
		RedisHost:        cfg.RedisHost,
		RedisPort:        cfg.RedisPort,
		RedisPassword:    cfg.RedisPassword,
		RedisDB:          cfg.RedisDB,
	})
	if err != nil {
		fmt.Printf("[StorageService] Fatal error initializing storage: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("[StorageService] Initialized successfully.")

	tempDir := filepath.Join(".", "temp_uploads")

	// 4. Start gRPC Server
	gServer, gLis, err := grpcServer.StartGRPCServer(cfg.GRPCPort, svc, tempDir)
	if err != nil {
		fmt.Printf("[gRPC] Fatal error starting gRPC server: %v\n", err)
		os.Exit(1)
	}

	go func() {
		fmt.Printf("[gRPC] Storage gRPC worker listening on 0.0.0.0:%d\n", cfg.GRPCPort)
		if err := gServer.Serve(gLis); err != nil {
			fmt.Printf("[gRPC] Server error: %v\n", err)
		}
	}()

	// 5. Start HTTP Server
	hServer := httpServer.NewHTTPServer(svc, tempDir)
	httpHandler := hServer.SetupRoutes()

	httpSrv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:      httpHandler,
		ReadTimeout:  120 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		fmt.Printf("[HTTP] Storage HTTP worker listening on port %d\n", cfg.HTTPPort)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[HTTP] Server error: %v\n", err)
		}
	}()

	// 6. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("[Server] Shutting down gracefully...")
	gServer.GracefulStop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
	fmt.Println("[Server] Stopped.")
}
