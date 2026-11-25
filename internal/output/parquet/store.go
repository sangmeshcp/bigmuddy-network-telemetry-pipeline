package parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"

	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/config"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/pipeline"
)

// objectStore is where finished files go. Put must be atomic: readers see
// either no object or the complete file.
type objectStore interface {
	Put(ctx context.Context, key string, data []byte) error
}

// --- local filesystem ---------------------------------------------------------

type fileStore struct{ root string }

func (f *fileStore) Put(_ context.Context, key string, data []byte) error {
	path := filepath.Join(f.root, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path) // atomic: never a half-written .parquet
}

// --- S3-compatible ------------------------------------------------------------

type s3Store struct {
	client *minio.Client
	bucket string
	opts   minio.PutObjectOptions
}

func newS3Store(bucket string, c S3) (*s3Store, error) {
	endpoint := c.Endpoint
	if endpoint == "" {
		endpoint = "s3.amazonaws.com"
	}
	secret, err := config.Secret(c.SecretKey, c.SecretKeyFile)
	if err != nil {
		return nil, fmt.Errorf("s3 secret key: %w", err)
	}
	var creds *credentials.Credentials
	if c.AccessKey != "" {
		creds = credentials.NewStaticV4(c.AccessKey, secret, c.SessionToken)
	} else {
		// Standard chain: env vars, web identity (IRSA / workload identity
		// federation), shared files, instance/task role.
		creds = credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvAWS{}, &credentials.FileAWSCredentials{}, &credentials.IAM{}})
	}
	tc, err := c.TLS.Client()
	if err != nil {
		return nil, err
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = tc
	lookup := minio.BucketLookupAuto
	if c.PathStyle {
		lookup = minio.BucketLookupPath
	}
	client, err := minio.New(endpoint, &minio.Options{Creds: creds, Secure: !c.Insecure, Region: c.Region,
		BucketLookup: lookup, Transport: tr})
	if err != nil {
		return nil, err
	}
	opts := minio.PutObjectOptions{ContentType: "application/vnd.apache.parquet", StorageClass: c.StorageClass}
	switch c.ServerSideCrypt {
	case "":
	case "AES256":
		opts.ServerSideEncryption = encrypt.NewSSE()
	default:
		return nil, fmt.Errorf("s3.sse must be empty or AES256")
	}
	return &s3Store{client: client, bucket: bucket, opts: opts}, nil
}

func (s *s3Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), s.opts)
	if err == nil {
		return nil
	}
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		switch resp.Code {
		case "NoSuchBucket", "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidBucketName":
			return pipeline.Permanent(fmt.Errorf("s3 put %s: %w", key, err))
		}
	}
	return fmt.Errorf("s3 put %s: %w", key, err)
}
