package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/fsnotify/fsnotify"
)

// TODO: make object / Type for passing around s3manager and s3 svc
var (
	s3Bucket      string = "vandelay-media"
	s3EndpointUrl string = "s3.us-west-000.backblazeb2.com"
	s3Region      string = "us-west-000" // TODO: parse from Endpoint?
	watchPath     string = "./mount"
	// s3Client      *s3.S3
	// s3Uploader    *s3manager.Uploader
)

// func objectExists(uploader *Uploader, objectKey string) (bool, error) {
// 	input := &s3.HeadObjectInput{
// 		Bucket: aws.String(s3Bucket),
// 		Key:    aws.String(objectKey),
// 	}
//
// 	_, err := s3Client.HeadObject(input)
// 	if err != nil {
// 		if aerr, ok := err.(awserr.Error); ok {
// 			switch aerr.Code() {
// 			case "NotFound":
// 				return false, nil
// 			default:
// 				return false, err
// 			}
// 		}
// 	}
// 	return true, nil
// }

// func uploadFile(uploader *Uploader, localPath string) error {
// 	relPath, err := filepath.Rel(watchPath, localPath)
// 	if err != nil {
// 		return err
// 	}
//
// 	file, err := os.Open(localPath)
// 	if err != nil {
// 		return err
// 	}
//
// 	upParams := &s3manager.UploadInput{
// 		Bucket: &s3Bucket,
// 		Key:    aws.String(relPath),
// 		Body:   file,
// 	}
//
// 	return uploader.Upload(
//
// 	exists, err := objectExists(relPath)
// 	if err != nil {
// 		return err
// 	}
//
// 	if exists {
// 		log.Printf("object exists, not uploading: %s\n", relPath)
// 		return nil
// 	} else {
// 		log.Printf("uploading %s -> s3://%s/%s\n", localPath, s3Bucket, relPath)
// 	}
//
// 	_, err = s3Uploader.Upload(upParams)
// 	return err
// }

func processDir(folderPath string) error {
	files, err := filesToUpload(folderPath)
	if err != nil {
		return err
	}
	for _, file := range files {
		statInfo, err := os.Stat(file)
		if err != nil {
			// TODO: catch individual errors, attempt processing other files
			return err
		}
		if statInfo.Mode().IsRegular() {
			log.Printf("%s\n", file)
			// err := uploadFile(file)
			// if err != nil {
			// 	log.Printf("uploadFile error: %s\n", err)
			// }
		}
	}
	return nil
}

func filesToUpload(folderPath string) ([]string, error) {
	var files []string
	err := filepath.Walk(folderPath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			files = append(files, path)
			return nil
		})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func main() {
	sess, err := session.NewSession(&aws.Config{
		Endpoint: aws.String(s3EndpointUrl),
		Region:   aws.String(s3Region),
	})
	if err != nil {
		log.Fatalf("failed to load config, %v", err)
	}

	s3Client := s3.New(sess)
	uploader := NewUploader(s3Client, s3manager.NewUploaderWithClient(s3Client))

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					statInfo, err := os.Stat(event.Name)
					if err != nil {
						log.Fatalf("error: %s\n", err)
					}
					if statInfo.IsDir() {
						log.Printf("directory created: %s\n", event.Name)
						err := processDir(event.Name)
						if err != nil {
							log.Printf("error: %s\n", err)
						}
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	err = watcher.Add(watchPath)
	if err != nil {
		log.Fatal(err)
	}
	<-done
}

// S3APIClient provides interface for implementations to provide S3 HeadObject functionality.
type S3APIClient interface {
	HeadObjectWithContext(context.Context, *s3.HeadObjectInput, ...request.Option) (*s3.HeadObjectOutput, error)
}

// S3Uploader provides interface for implementatison to provide S3 Upload manager functionality.
type S3Uploader interface {
	UploadWithContext(context.Context, *s3manager.UploadInput, ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
}

// Uploader is a custom S3 object uploader that couples checking if object exists before uploading a new value.
type Uploader struct {
	// Interfaces are used so that unit tetss can stub out the lower level client,
	// and internal behavior of the SDK.
	s3Client   S3APIClient
	s3Uploader S3Uploader
}

// New Uploader returns a new instance of the Uploader
func NewUploader(s3Client S3APIClient, s3Uploader S3Uploader) *Uploader {
	return &Uploader{
		s3Uploader: S3Uploader,
		s3Client:   s3Client,
	}
}

// Upload uploads the object to S3 bucket and key if it does not exists, otherwise returns error.
func (m *Uploader) Upload(ctx context.Context, bucket, key string, object io.ReadSeeker) error {
	// Check object exists before uploading new one
	return fmt.Errorf("not implemented")
}
