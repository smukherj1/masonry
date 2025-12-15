package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	pb "github.com/smukherj1/masonry/client/generated"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	uploadFile = flag.String("upload-file", "", "File to upload")
	uploadID   = flag.String("upload-id", "", "Upload ID")
	serverAddr = flag.String("server-addr", "localhost:4000", "Server address")
)

type clients struct {
	blobsUploadClient pb.BlobsUploadServiceClient
	blobsClient       pb.BlobServiceClient
}

func newClients() (*clients, func() error, error) {
	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("unable to dial server at %v: %w", *serverAddr, err)
	}
	defer conn.Close()
	return &clients{
		blobsUploadClient: pb.NewBlobsUploadServiceClient(conn),
		blobsClient:       pb.NewBlobServiceClient(conn),
	}, conn.Close, nil
}

func doUpload(ctx context.Context, clients *clients, uploadID, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("unable to open file %q for uploading: %w", filePath, err)
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("unable to get file info for %q: %w", filePath, err)
	}
	log.Printf("Starting to upload file %q with upload ID %q, size %v\n", filePath, uploadID, fileInfo.Size())
	uc := clients.blobsUploadClient
	stream, err := uc.Upload(ctx)
	if err != nil {
		return fmt.Errorf("unable to initiate upload: %w", err)
	}
	offset := uint64(0)
	// Stay within the 4 MB GRPC request limit.
	chunk := make([]byte, 4*1000*1000-1000)
	for {
		nBytes, err := file.Read(chunk)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading file %q while uploading, %v bytes read so far: %w", filePath, offset, err)
		}
		log.Printf("Uploading chunk %v/%v bytes read so far\n", offset, fileInfo.Size())
		if err := stream.Send(&pb.BlobUploadRequest{
			UploadId: uploadID,
			Data:     chunk[:nBytes],
			Offset:   offset,
		}); err != nil {
			return fmt.Errorf("unable to upload: %w", err)
		}
		offset += uint64(nBytes)
	}
	uploadResp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("unable to upload: %w", err)
	}
	log.Printf("Upload completed with upload ID %q, total bytes uploaded %v/%v\n", uploadID, uploadResp.NextOffset, fileInfo.Size())

	completeResp, err := uc.Complete(ctx, &pb.BlobUploadCompleteRequest{
		UploadId: uploadID,
	})
	if err != nil {
		return fmt.Errorf("unable to complete upload: %w", err)
	}
	log.Printf("Upload completed with upload ID %q, digest %v/%v\n", uploadID, completeResp.Digest.Hash, completeResp.Digest.SizeBytes)
	return nil
}

func main() {
	flag.Parse()
	if *uploadFile == "" {
		log.Fatal("Missing upload-file")
	}
	if *uploadID == "" {
		log.Fatal("Missing upload-id")
	}

	clients, close, err := newClients()
	if err != nil {
		log.Fatalf("Unable to create clients: %v", err)
	}
	defer close()

	ctx := context.Background()
	if err := doUpload(ctx, clients, *uploadID, *uploadFile); err != nil {
		log.Fatalf("Unable to upload: %v", err)
	}
}
