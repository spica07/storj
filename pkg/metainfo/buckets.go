// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo

import (
	"bytes"
	"context"
	"time"

	"storj.io/storj/pkg/paths"
	"storj.io/storj/pkg/storage/buckets"
	"storj.io/storj/pkg/storage/meta"
	"storj.io/storj/pkg/storage/objects"
	"storj.io/storj/pkg/storj"
)

type Buckets struct {
	store objects.Store
}

// CreateBucket creates a new bucket with the specified information
func (db *Buckets) CreateBucket(ctx context.Context, bucket string, info *storj.Bucket) (storj.Bucket, error) {
	if bucket == "" {
		return storj.Bucket{}, buckets.NoBucketError.New("")
	}

	var reader bytes.Reader

	var exp time.Time
	meta, err := db.store.Put(ctx, paths.New(bucket), &reader, objects.SerializableMeta{}, exp)
	if err != nil {
		return storj.Bucket{}, err
	}

	return storj.Bucket{
		Name:    bucket,
		Created: meta.Modified,
	}, nil
}

// DeleteBucket deletes bucket
func (db *Buckets) DeleteBucket(ctx context.Context, bucket string) error {
	if bucket == "" {
		return buckets.NoBucketError.New("")
	}

	return db.store.Delete(ctx, paths.New(bucket))
}

// GetBucket gets bucket information
func (db *Buckets) GetBucket(ctx context.Context, bucket string) (storj.Bucket, error) {
	if bucket == "" {
		return storj.Bucket{}, buckets.NoBucketError.New("")
	}

	meta, err := db.store.Meta(ctx, paths.New(bucket))
	if err != nil {
		return storj.Bucket{}, err
	}

	return storj.Bucket{
		Name:    bucket,
		Created: meta.Modified,
	}, nil
}

// ListBuckets lists buckets
func (db *Buckets) ListBuckets(ctx context.Context, first string, limit int) (storj.BucketList, error) {
	// TODO: fix unable to get startAfter from first
	startAfter := paths.New(first)
	if len(startAfter) > 0 {
		last := []byte(startAfter[len(startAfter)-1])
		last[len(last)-1]--
		last = append(last, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)
		startAfter[len(startAfter)-1] = string(last)
	}

	items, more, err := db.store.List(ctx, nil, startAfter, nil, false, limit, meta.Modified)
	if err != nil {
		return storj.BucketList{}, err
	}

	list := storj.BucketList{
		NextFirst: "",
		More:      more,
	}

	for _, item := range items {
		list.Buckets = append(list.Buckets, storj.Bucket{
			Name:    item.Path.String(),
			Created: item.Meta.Modified,
		})
	}

	if len(list.Buckets) > 0 && more {
		list.NextFirst = list.Buckets[len(list.Buckets)-1].Name + "\x00"
	}

	return list, nil
}
