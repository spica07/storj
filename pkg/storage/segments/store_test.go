// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package segments

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"

	"storj.io/storj/pkg/dht"
	"storj.io/storj/pkg/eestream"
	mock_eestream "storj.io/storj/pkg/eestream/mocks"
	"storj.io/storj/pkg/node"
	"storj.io/storj/pkg/overlay"
	mock_overlay "storj.io/storj/pkg/overlay/mocks"
	"storj.io/storj/pkg/paths"
	"storj.io/storj/pkg/pb"
	"storj.io/storj/pkg/piecestore/rpc/client"
	pdb "storj.io/storj/pkg/pointerdb/pdbclient"
	mock_pointerdb "storj.io/storj/pkg/pointerdb/pdbclient/mocks"
	"storj.io/storj/pkg/ranger"
	mock_ecclient "storj.io/storj/pkg/storage/ec/mocks"
)

var (
	ctx = context.Background()
)

func TestNewSegmentStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockOC := mock_overlay.NewMockClient(ctrl)
	mockEC := mock_ecclient.NewMockClient(ctrl)
	mockPDB := mock_pointerdb.NewMockClient(ctrl)
	rs := eestream.RedundancyStrategy{
		ErasureScheme: mock_eestream.NewMockErasureScheme(ctrl),
	}

	ss := NewSegmentStore(mockOC, mockEC, mockPDB, rs, 10)
	assert.NotNil(t, ss)
}

func TestSegmentStoreMeta(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockOC := mock_overlay.NewMockClient(ctrl)
	mockEC := mock_ecclient.NewMockClient(ctrl)
	mockPDB := mock_pointerdb.NewMockClient(ctrl)
	rs := eestream.RedundancyStrategy{
		ErasureScheme: mock_eestream.NewMockErasureScheme(ctrl),
	}

	ss := segmentStore{mockOC, mockEC, mockPDB, rs, 10}
	assert.NotNil(t, ss)

	var mExp time.Time
	pExp, err := ptypes.TimestampProto(mExp)
	assert.NoError(t, err)

	for _, tt := range []struct {
		pathInput     string
		returnPointer *pb.Pointer
		returnMeta    Meta
	}{
		{"path/1/2/3", &pb.Pointer{CreationDate: pExp, ExpirationDate: pExp}, Meta{Modified: mExp, Expiration: mExp}},
	} {
		p := paths.New(tt.pathInput)

		calls := []*gomock.Call{
			mockPDB.EXPECT().Get(
				gomock.Any(), gomock.Any(),
			).Return(tt.returnPointer, nil),
		}
		gomock.InOrder(calls...)

		m, err := ss.Meta(ctx, p)
		assert.NoError(t, err)
		assert.Equal(t, m, tt.returnMeta)
	}
}

func TestSegmentStorePutRemote(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, tt := range []struct {
		name          string
		pathInput     string
		mdInput       []byte
		thresholdSize int
		expiration    time.Time
		readerContent string
	}{
		{"test remote put", "path/1", []byte("abcdefghijklmnopqrstuvwxyz"), 2, time.Unix(0, 0).UTC(), "readerreaderreader"},
	} {
		mockOC := mock_overlay.NewMockClient(ctrl)
		mockEC := mock_ecclient.NewMockClient(ctrl)
		mockPDB := mock_pointerdb.NewMockClient(ctrl)
		mockES := mock_eestream.NewMockErasureScheme(ctrl)
		rs := eestream.RedundancyStrategy{
			ErasureScheme: mockES,
		}

		ss := segmentStore{mockOC, mockEC, mockPDB, rs, tt.thresholdSize}
		assert.NotNil(t, ss)

		p := paths.New(tt.pathInput)
		r := strings.NewReader(tt.readerContent)

		calls := []*gomock.Call{
			mockES.EXPECT().TotalCount().Return(1),
			mockOC.EXPECT().Choose(
				gomock.Any(), gomock.Any(),
			).Return([]*pb.Node{
				{Id: "im-a-node"},
			}, nil),
			mockPDB.EXPECT().SignedMessage(),
			mockEC.EXPECT().Put(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			),
			mockES.EXPECT().RequiredCount().Return(1),
			mockES.EXPECT().TotalCount().Return(1),
			mockES.EXPECT().ErasureShareSize().Return(1),
			mockPDB.EXPECT().Put(
				gomock.Any(), gomock.Any(), gomock.Any(),
			).Return(nil),
			mockPDB.EXPECT().Get(
				gomock.Any(), gomock.Any(),
			),
		}
		gomock.InOrder(calls...)

		_, err := ss.Put(ctx, r, tt.expiration, func() (paths.Path, []byte, error) {
			return p, tt.mdInput, nil
		})
		assert.NoError(t, err, tt.name)
	}
}

func TestSegmentStorePutInline(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, tt := range []struct {
		name          string
		pathInput     string
		mdInput       []byte
		thresholdSize int
		expiration    time.Time
		readerContent string
	}{
		{"test inline put", "path/1", []byte("111"), 1000, time.Unix(0, 0).UTC(), "readerreaderreader"},
	} {
		mockOC := mock_overlay.NewMockClient(ctrl)
		mockEC := mock_ecclient.NewMockClient(ctrl)
		mockPDB := mock_pointerdb.NewMockClient(ctrl)
		mockES := mock_eestream.NewMockErasureScheme(ctrl)
		rs := eestream.RedundancyStrategy{
			ErasureScheme: mockES,
		}

		ss := segmentStore{mockOC, mockEC, mockPDB, rs, tt.thresholdSize}
		assert.NotNil(t, ss)

		p := paths.New(tt.pathInput)
		r := strings.NewReader(tt.readerContent)

		calls := []*gomock.Call{
			mockPDB.EXPECT().Put(
				gomock.Any(), gomock.Any(), gomock.Any(),
			).Return(nil),
			mockPDB.EXPECT().Get(
				gomock.Any(), gomock.Any(),
			),
		}
		gomock.InOrder(calls...)

		_, err := ss.Put(ctx, r, tt.expiration, func() (paths.Path, []byte, error) {
			return p, tt.mdInput, nil
		})
		assert.NoError(t, err, tt.name)
	}
}

func TestSegmentStoreGetInline(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ti := time.Unix(0, 0).UTC()
	someTime, err := ptypes.TimestampProto(ti)
	assert.NoError(t, err)

	for _, tt := range []struct {
		pathInput     string
		thresholdSize int
		pointerType   pb.Pointer_DataType
		inlineContent []byte
		size          int64
		metadata      []byte
	}{
		{"path/1/2/3", 10, pb.Pointer_INLINE, []byte("000"), int64(3), []byte("metadata")},
	} {
		mockOC := mock_overlay.NewMockClient(ctrl)
		mockEC := mock_ecclient.NewMockClient(ctrl)
		mockPDB := mock_pointerdb.NewMockClient(ctrl)
		mockES := mock_eestream.NewMockErasureScheme(ctrl)
		rs := eestream.RedundancyStrategy{
			ErasureScheme: mockES,
		}

		ss := segmentStore{mockOC, mockEC, mockPDB, rs, tt.thresholdSize}
		assert.NotNil(t, ss)

		p := paths.New(tt.pathInput)

		calls := []*gomock.Call{
			mockPDB.EXPECT().Get(
				gomock.Any(), gomock.Any(),
			).Return(&pb.Pointer{
				Type:           tt.pointerType,
				InlineSegment:  tt.inlineContent,
				CreationDate:   someTime,
				ExpirationDate: someTime,
				Size:           tt.size,
				Metadata:       tt.metadata,
			}, nil),
		}
		gomock.InOrder(calls...)

		_, _, err := ss.Get(ctx, p)
		assert.NoError(t, err)
	}
}

func TestSegmentStoreRepairRemote(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ti := time.Unix(0, 0).UTC()
	someTime, err := ptypes.TimestampProto(ti)
	assert.NoError(t, err)

	for _, tt := range []struct {
		pathInput               string
		thresholdSize           int
		pointerType             pb.Pointer_DataType
		size                    int64
		metadata                []byte
		pieceID                 string
		lostPieces              []int
		remotePieces            []*pb.RemotePiece
		originalNodes           []*pb.Node
		originalIDs             []dht.NodeID
		newNodes                []*pb.Node
		successfulNodes         []*pb.Node
		data                    string
		strsize, offset, length int64
		substr                  string
		meta                    Meta
	}{
		{
			pathInput:     "path/1/2/3",
			thresholdSize: 10,
			pointerType:   pb.Pointer_REMOTE,
			size:          int64(3),
			metadata:      []byte("metadata"),
			pieceID:       "myPieceID",
			lostPieces:    []int{1, 2},
			remotePieces: []*pb.RemotePiece{
				{PieceNum: 0, NodeId: "0"}, {PieceNum: 1, NodeId: "1"}, {PieceNum: 2, NodeId: "2"}, {PieceNum: 3, NodeId: "3"},
				{PieceNum: 4, NodeId: "4"}, {PieceNum: 5, NodeId: "5"}, {PieceNum: 6, NodeId: "6"}, {PieceNum: 7, NodeId: "7"},
			},
			originalNodes: []*pb.Node{{Id: "0"}, {Id: "1"}, {Id: "2"}, {Id: "3"}, {Id: "4"}, {Id: "5"}, {Id: "6"}, {Id: "7"}},
			originalIDs: []dht.NodeID{
				node.IDFromString("0"), node.IDFromString("1"), node.IDFromString("2"), node.IDFromString("3"),
				node.IDFromString("4"), node.IDFromString("5"), node.IDFromString("6"), node.IDFromString("7"),
			},
			newNodes:        []*pb.Node{{Id: "8"}, {Id: "9"}},
			successfulNodes: []*pb.Node{nil, {Id: "9"}, {Id: "8"}, nil, nil, nil, nil, nil},
			data:            "abcdefghijkl",
			strsize:         12,
			offset:          1,
			length:          4,
			substr:          "bcde",
			meta:            Meta{},
		},
	} {
		mockOC := mock_overlay.NewMockClient(ctrl)
		mockEC := mock_ecclient.NewMockClient(ctrl)
		mockPDB := mock_pointerdb.NewMockClient(ctrl)
		scheme := &pb.RedundancyScheme{
			Type:             pb.RedundancyScheme_RS,
			MinReq:           4,
			Total:            8,
			RepairThreshold:  7,
			SuccessThreshold: 6,
			ErasureShareSize: 1024,
		}
		es, err := makeErasureScheme(scheme)
		assert.NoError(t, err)

		rs := eestream.RedundancyStrategy{
			ErasureScheme: es,
		}

		ss := segmentStore{mockOC, mockEC, mockPDB, rs, tt.thresholdSize}
		assert.NotNil(t, ss)

		p := paths.New(tt.pathInput)

		exp, err := ptypes.Timestamp(someTime)
		assert.NoError(t, err)

		rr := ranger.ByteRanger([]byte(tt.data))
		r, err := rr.Range(ctx, 0, rr.Size())
		assert.NoError(t, err)

		calls := []*gomock.Call{
			mockPDB.EXPECT().Get(
				gomock.Any(), p,
			).Return(&pb.Pointer{
				Type: tt.pointerType,
				Remote: &pb.RemoteSegment{
					Redundancy:   scheme,
					PieceId:      tt.pieceID,
					RemotePieces: tt.remotePieces,
				},
				CreationDate:   someTime,
				ExpirationDate: someTime,
				Size:           tt.size,
				Metadata:       tt.metadata,
			}, nil),
			mockOC.EXPECT().BulkLookup(gomock.Any(), gomock.Any()).Return(tt.originalNodes, nil),
			mockOC.EXPECT().Choose(gomock.Any(), overlay.Options{Amount: 2, Space: 0, Excluded: tt.originalIDs}).Return(tt.newNodes, nil),
			mockPDB.EXPECT().SignedMessage(),
			mockEC.EXPECT().Get(
				gomock.Any(), gomock.Any(), es, client.PieceID(tt.pieceID), tt.size, gomock.Any(),
			).Return(rr, nil),
			mockEC.EXPECT().Put(
				gomock.Any(), tt.successfulNodes, rs, client.PieceID(tt.pieceID), r, exp, gomock.Any(),
			).Return(tt.successfulNodes, nil),
			mockPDB.EXPECT().Put(
				gomock.Any(), p, gomock.Any(),
			).Return(nil),
		}
		gomock.InOrder(calls...)

		err = ss.Repair(ctx, p, tt.lostPieces)
		assert.NoError(t, err)
	}
}

func TestSegmentStoreGetRemote(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ti := time.Unix(0, 0).UTC()
	someTime, err := ptypes.TimestampProto(ti)
	assert.NoError(t, err)

	for _, tt := range []struct {
		pathInput     string
		thresholdSize int
		pointerType   pb.Pointer_DataType
		size          int64
		metadata      []byte
	}{
		{"path/1/2/3", 10, pb.Pointer_REMOTE, int64(3), []byte("metadata")},
	} {
		mockOC := mock_overlay.NewMockClient(ctrl)
		mockEC := mock_ecclient.NewMockClient(ctrl)
		mockPDB := mock_pointerdb.NewMockClient(ctrl)
		mockES := mock_eestream.NewMockErasureScheme(ctrl)
		rs := eestream.RedundancyStrategy{
			ErasureScheme: mockES,
		}

		ss := segmentStore{mockOC, mockEC, mockPDB, rs, tt.thresholdSize}
		assert.NotNil(t, ss)

		p := paths.New(tt.pathInput)

		calls := []*gomock.Call{
			mockPDB.EXPECT().Get(
				gomock.Any(), gomock.Any(),
			).Return(&pb.Pointer{
				Type: tt.pointerType,
				Remote: &pb.RemoteSegment{
					Redundancy: &pb.RedundancyScheme{
						Type:             pb.RedundancyScheme_RS,
						MinReq:           1,
						Total:            2,
						RepairThreshold:  1,
						SuccessThreshold: 2,
					},
					PieceId:      "here's my piece id",
					RemotePieces: []*pb.RemotePiece{},
				},
				CreationDate:   someTime,
				ExpirationDate: someTime,
				Size:           tt.size,
				Metadata:       tt.metadata,
			}, nil),
			mockOC.EXPECT().BulkLookup(gomock.Any(), gomock.Any()),
			mockPDB.EXPECT().SignedMessage(),
			mockEC.EXPECT().Get(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			),
		}
		gomock.InOrder(calls...)

		_, _, err := ss.Get(ctx, p)
		assert.NoError(t, err)
	}
}

func TestSegmentStoreDeleteInline(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ti := time.Unix(0, 0).UTC()
	someTime, err := ptypes.TimestampProto(ti)
	assert.NoError(t, err)

	for _, tt := range []struct {
		pathInput     string
		thresholdSize int
		pointerType   pb.Pointer_DataType
		inlineContent []byte
		size          int64
		metadata      []byte
	}{
		{"path/1/2/3", 10, pb.Pointer_INLINE, []byte("000"), int64(3), []byte("metadata")},
	} {
		mockOC := mock_overlay.NewMockClient(ctrl)
		mockEC := mock_ecclient.NewMockClient(ctrl)
		mockPDB := mock_pointerdb.NewMockClient(ctrl)
		mockES := mock_eestream.NewMockErasureScheme(ctrl)
		rs := eestream.RedundancyStrategy{
			ErasureScheme: mockES,
		}

		ss := segmentStore{mockOC, mockEC, mockPDB, rs, tt.thresholdSize}
		assert.NotNil(t, ss)

		p := paths.New(tt.pathInput)

		calls := []*gomock.Call{
			mockPDB.EXPECT().Get(
				gomock.Any(), gomock.Any(),
			).Return(&pb.Pointer{
				Type:           tt.pointerType,
				InlineSegment:  tt.inlineContent,
				CreationDate:   someTime,
				ExpirationDate: someTime,
				Size:           tt.size,
				Metadata:       tt.metadata,
			}, nil),
			mockPDB.EXPECT().Delete(
				gomock.Any(), gomock.Any(),
			),
		}
		gomock.InOrder(calls...)

		err := ss.Delete(ctx, p)
		assert.NoError(t, err)
	}
}

func TestSegmentStoreDeleteRemote(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ti := time.Unix(0, 0).UTC()
	someTime, err := ptypes.TimestampProto(ti)
	assert.NoError(t, err)

	for _, tt := range []struct {
		pathInput     string
		thresholdSize int
		pointerType   pb.Pointer_DataType
		size          int64
		metadata      []byte
	}{
		{"path/1/2/3", 10, pb.Pointer_REMOTE, int64(3), []byte("metadata")},
	} {
		mockOC := mock_overlay.NewMockClient(ctrl)
		mockEC := mock_ecclient.NewMockClient(ctrl)
		mockPDB := mock_pointerdb.NewMockClient(ctrl)
		mockES := mock_eestream.NewMockErasureScheme(ctrl)
		rs := eestream.RedundancyStrategy{
			ErasureScheme: mockES,
		}

		ss := segmentStore{mockOC, mockEC, mockPDB, rs, tt.thresholdSize}
		assert.NotNil(t, ss)

		p := paths.New(tt.pathInput)

		calls := []*gomock.Call{
			mockPDB.EXPECT().Get(
				gomock.Any(), gomock.Any(),
			).Return(&pb.Pointer{
				Type: tt.pointerType,
				Remote: &pb.RemoteSegment{
					Redundancy: &pb.RedundancyScheme{
						Type:             pb.RedundancyScheme_RS,
						MinReq:           1,
						Total:            2,
						RepairThreshold:  1,
						SuccessThreshold: 2,
					},
					PieceId:      "here's my piece id",
					RemotePieces: []*pb.RemotePiece{},
				},
				CreationDate:   someTime,
				ExpirationDate: someTime,
				Size:           tt.size,
				Metadata:       tt.metadata,
			}, nil),
			mockOC.EXPECT().BulkLookup(gomock.Any(), gomock.Any()),
			mockPDB.EXPECT().SignedMessage(),
			mockEC.EXPECT().Delete(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			),
			mockPDB.EXPECT().Delete(
				gomock.Any(), gomock.Any(),
			),
		}
		gomock.InOrder(calls...)

		err := ss.Delete(ctx, p)
		assert.NoError(t, err)
	}
}

func TestSegmentStoreList(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, tt := range []struct {
		prefixInput     string
		startAfterInput string
		thresholdSize   int
		itemPath        string
		inlineContent   []byte
		metadata        []byte
	}{
		{"bucket1", "s0/path/1", 10, "s0/path/1", []byte("inline"), []byte("metadata")},
	} {
		mockOC := mock_overlay.NewMockClient(ctrl)
		mockEC := mock_ecclient.NewMockClient(ctrl)
		mockPDB := mock_pointerdb.NewMockClient(ctrl)
		mockES := mock_eestream.NewMockErasureScheme(ctrl)
		rs := eestream.RedundancyStrategy{
			ErasureScheme: mockES,
		}

		ss := segmentStore{mockOC, mockEC, mockPDB, rs, tt.thresholdSize}
		assert.NotNil(t, ss)

		prefix := paths.New(tt.prefixInput)
		startAfter := paths.New(tt.startAfterInput)
		listedPath := paths.New(tt.itemPath)

		ti := time.Unix(0, 0).UTC()
		someTime, err := ptypes.TimestampProto(ti)
		assert.NoError(t, err)

		calls := []*gomock.Call{
			mockPDB.EXPECT().List(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
				gomock.Any(), gomock.Any(), gomock.Any(),
			).Return([]pdb.ListItem{
				{
					Path: listedPath,
					Pointer: &pb.Pointer{
						Type:           pb.Pointer_INLINE,
						InlineSegment:  tt.inlineContent,
						CreationDate:   someTime,
						ExpirationDate: someTime,
						Size:           int64(4),
						Metadata:       tt.metadata,
					},
				},
			}, true, nil),
		}
		gomock.InOrder(calls...)

		_, _, err = ss.List(ctx, prefix, startAfter, nil, false, 10, uint32(1))
		assert.NoError(t, err)
	}
}
