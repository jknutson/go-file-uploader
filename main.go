package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
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
	s3Client      *s3.S3
	s3Uploader    *s3manager.Uploader
)

func objectExists(objectKey string) (bool, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(objectKey),
	}

	_, err := s3Client.HeadObject(input)
	if err != nil {
		return false, err
	}
	return true, nil
}

func uploadFile(localPath string) error {
	relPath, err := filepath.Rel(watchPath, localPath)
	if err != nil {
		return err
	}

	file, err := os.Open(localPath)
	if err != nil {
		return err
	}

	upParams := &s3manager.UploadInput{
		Bucket: &s3Bucket,
		Key:    aws.String(localPath),
		Body:   file,
	}

	exists, err := objectExists(relPath)
	if err != nil {
		return err
	}

	if exists {
		log.Printf("object exists, not uploading: %s\n", relPath)
		return nil
	} else {
		log.Printf("uploading %s -> s3://%s/%s\n", localPath, s3Bucket, relPath)
	}

	if false { // testing
		// TODO: check if file exists first
		_, err = s3Uploader.Upload(upParams)
	} else {
		return nil
	}
	return err
}

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
			return uploadFile(file)
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

	s3Client = s3.New(sess)
	s3Uploader = s3manager.NewUploader(sess)

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
				switch event.Op {
				case fsnotify.Create:
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
