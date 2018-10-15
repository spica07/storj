// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
	"storj.io/storj/pkg/storage/buckets"
	"storj.io/storj/pkg/storage/objects"
	"storj.io/storj/pkg/storage/streams"
	"storj.io/storj/pkg/storj"
)

type Objects struct {
	store streams.Store
}

// GetObject returns information about an object
func (db *Objects) GetObject(ctx context.Context, bucket string, path storj.Path) (storj.Object, error) {
	if bucket == "" {
		return storj.Object{}, buckets.NoBucketError.New("")
	}
	if path == "" {
		return storj.Object{}, objects.NoPathError.New("")
	}

	_, meta, err := db.store.Get(ctx, objectPath(bucket, path))
	if err != nil {
		return storj.Object{}, err
	}

	return objectFromMeta(bucket, path, meta), err
}

func objectFromMeta(bucket string, path storj.Path, meta streams.Meta) storj.Object {
	ser := objects.SerializableMeta{}
	err := proto.Unmarshal(meta.Data, &ser)
	if err != nil {
		zap.S().Warnf("Failed deserializing metadata: %v", err)
	}

	return storj.Object{
		Bucket: bucket,
		Path:   path,

		Modified: meta.Modified,
		Expires:  meta.Expiration,

		ContentType: ser.ContentType,
		UserDefined: ser.UserDefined,

		Stream: storj.Stream{
			Size: meta.Size,
			// TODO: other fields
		},
	}
}
