package main

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"net/url"
	"os"
	"path"
	"sync"
)

type Bucket struct {
	_ struct{} `type:"structure"`

	// URI (s3://bucket/prefix/
	URI *string `type:"*string" required:"true"`

	// AWS Profile to auth with
	AWSProfile *string `type:"*string" required:"true"`

	// url representation for URI
	url         *url.URL
	region      *string
	objects     *[]*s3.Object
	svc         *s3.S3
	objectsDiff []*s3.Object
}

func (b Bucket) SVC() *s3.S3 {
	if b.svc == nil {
		config := &aws.Config{
			Region:      b.Region(),
			Credentials: credentials.NewSharedCredentials("", *b.AWSProfile),
		}
		b.svc = s3.New(session.Must(session.NewSession(config)))
	}
	return b.svc
}

func (b Bucket) URL() *url.URL {
	if b.url == nil {
		res, err := url.Parse(*b.URI)
		if err != nil {
			panic(err)
		}
		b.url = res
	}

	return b.url
}

func (b Bucket) Bucket() *string {
	return &b.URL().Host
}

func (b Bucket) Prefix() *string {
	key := b.URL().Path[1:]
	return &key
}

func (b Bucket) Key(object *s3.Object) *string {
	return aws.String(path.Join(*b.Bucket(), *object.Key))
}

func (b Bucket) Region() *string {
	if b.region == nil {
		region, err := s3manager.GetBucketRegion(
			aws.BackgroundContext(), session.Must(session.NewSession()),
			*b.Bucket(), "us-east-1")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to find URI %s's region not found\n", b.URI)
		}
		b.region = &region
	}

	return b.region
}

func (b Bucket) Objects() *[]*s3.Object {
	if b.objects == nil {
		b.SVC().Config.Region = b.Region()
		input := &s3.ListObjectsV2Input{
			Bucket: b.Bucket(),
			Prefix: b.Prefix(),
		}

		output, err := b.SVC().ListObjectsV2(input)
		if err != nil {
			panic(b.URI)
		}
		b.objects = &output.Contents
	}
	return b.objects
}

func (b Bucket) Contains(dstBucket *Bucket, srcObject *s3.Object) bool {

	for _, v := range *dstBucket.Hashes() {
		if *v == *b.Hash(srcObject) {
			return true
		}
	}
	return false

}

func (b Bucket) Hash(object *s3.Object) *string {
	var bb bytes.Buffer

	bb.WriteByte(byte(*object.Size))
	bb.WriteString(*object.Key)

	hash := fmt.Sprintf("md5:%x", md5.Sum(bb.Bytes()))
	return &hash
}

func (b Bucket) Hashes() *[]*string {

	var hashes []*string
	for _, object := range *b.Objects() {
		hashes = append(hashes, b.Hash(object))
	}
	return &hashes

}

func (b Bucket) ObjectsDiff(dstBucket *Bucket) []*s3.Object {
	if b.objectsDiff == nil {
		for _, srcObject := range *b.Objects() {
			if !b.Contains(dstBucket, srcObject) {
				b.objectsDiff = append(b.objectsDiff, srcObject)
			}
		}
	}
	return b.objectsDiff
}

func (b Bucket) S3Sync(dstBucket *Bucket) {

	if len(b.ObjectsDiff(dstBucket)) == 0 {
		fmt.Printf("Content is identical\n")
		return
	}

	var wg sync.WaitGroup

	for _, object := range b.ObjectsDiff(dstBucket) {
		wg.Add(1)

		go func(object *s3.Object, srcBucket *Bucket, dstBucket *Bucket) {
			defer wg.Done()

			fmt.Printf("Copying %s%s --> %s\n", *srcBucket.URI, path.Base(*object.Key), *dstBucket.URI)

			input := &s3.CopyObjectInput{
				Bucket:     dstBucket.Bucket(),
				CopySource: aws.String(path.Join(*srcBucket.Bucket(), *object.Key)),
				Key:        aws.String(path.Join(*dstBucket.Prefix(), path.Base(*object.Key))),
			}
			headObjectInput := &s3.HeadObjectInput{
				Bucket: input.Bucket,
				Key:    input.Key,
			}

			_, err := dstBucket.SVC().CopyObject(input)
			if err != nil {
				fmt.Println("Failed to copy file", err)
				return
			}

			if dstBucket.SVC().WaitUntilObjectExists(headObjectInput) != nil {
				fmt.Println("Failed to ensure the file exists", err)
				return
			}
		}(object, &b, dstBucket)
	}
	wg.Wait()

}

func main() {

	SOURCE_URI := os.Getenv("SOURCE_URI")
	DESTINAION_URI := os.Getenv("DESTINATION_URI")
	AWS_PROFILE := os.Getenv("AWS_PROFILE")

	srcBucket := &Bucket{URI: &SOURCE_URI, AWSProfile: &AWS_PROFILE}
	dstBucket := &Bucket{URI: &DESTINAION_URI, AWSProfile: &AWS_PROFILE}

	srcBucket.S3Sync(dstBucket)

}
