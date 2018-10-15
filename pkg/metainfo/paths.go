// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo

import (
	"storj.io/storj/pkg/paths"
	"storj.io/storj/pkg/storj"
)

func bucketPath(bucket string) paths.Path {
	return paths.New(bucket)
}

func objectPath(bucket string, path storj.Path) paths.Path {
	return paths.New(path).Prepend(bucket)
}
