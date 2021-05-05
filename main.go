package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/fsnotify/fsnotify"
)

// TODO: make object / Type for passing around s3manager and s3 svc
var (
	s3Bucket      string = "vandelay-media"
	s3EndpointUrl string = "s3.us-west-000.backblazeb2.com"
	s3Region      string = "us-west-000" // TODO: parse from Endpoint?
	watchPath     string = "./mount"
)

func uploadFile(uploader *s3manager.Uploader, localPath string) error {
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

	if false { // testing
		// TODO: check if file exists first
		_, err = uploader.Upload(upParams)
	} else {
		log.Printf("%s -> s3://%s/%s\n", localPath, s3Bucket, relPath)
		return nil
	}
	return err
}

func processDir(uploader *s3manager.Uploader, folderPath string) error {
	files, err := filesToUpload(folderPath)
	if err != nil {
		return err
	}
	for _, file := range files {
		statInfo, err := os.Stat(file)
		if err != nil {
			// TODO: catch individual errors, attempt processing other files
			return nil
		}
		if statInfo.Mode().IsRegular() {
			uploadFile(uploader, file)
			log.Printf("%s\n", file)
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
	})

	uploader := s3manager.NewUploader(sess)

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
						processDir(uploader, event.Name)
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
