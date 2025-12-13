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
)

func doUpload(ctx context.Context, client pb.BlobsServiceClient, uploadID, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("unable to open file %q for uploading: %w", filePath, err)
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("unable to get file info for %q: %w", filePath, err)
	}
	beginResp, err := client.BeginUpload(ctx, &pb.BeginUploadRequest{
		UploadId: uploadID,
	})
	if err != nil {
		return fmt.Errorf("unable to begin upload: %w", err)
	}
	log.Printf("Starting to upload file %q with upload ID %q, size %v\n", filePath, beginResp.UploadId, fileInfo.Size())
	stream, err := client.Upload(ctx)
	if err != nil {
		return fmt.Errorf("unable to upload: %w", err)
	}
	offset := uint64(0)
	for {
		chunk := make([]byte, 100000)
		_, err := file.Read(chunk)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading file %q while uploading, %v bytes read so far: %w", filePath, offset, err)
		}
		log.Printf("Uploading chunk %v/%v bytes read so far\n", offset, fileInfo.Size())
		if err := stream.Send(&pb.UploadRequest{
			UploadId: beginResp.UploadId,
			Data:     chunk,
			Offset:   offset,
		}); err != nil {
			return fmt.Errorf("unable to upload: %w", err)
		}
		offset += uint64(len(chunk))
	}
	uploadResp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("unable to upload: %w", err)
	}
	log.Printf("Upload completed with upload ID %q, total bytes uploaded %v/%v\n", uploadResp.UploadId, uploadResp.NextOffset, fileInfo.Size())

	completeResp, err := client.CompleteUpload(ctx, &pb.CompleteUploadRequest{
		UploadId: beginResp.UploadId,
	})
	if err != nil {
		return fmt.Errorf("unable to complete upload: %w", err)
	}
	log.Printf("Upload completed with upload ID %q, digest %v/%v\n", completeResp.UploadId, completeResp.Digest.Hash, completeResp.Digest.SizeBytes)
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
	ctx := context.Background()
	conn, err := grpc.NewClient("localhost:4000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Unable to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewBlobsServiceClient(conn)

	if err := doUpload(ctx, client, *uploadID, *uploadFile); err != nil {
		log.Fatalf("Unable to upload: %v", err)
	}
}
