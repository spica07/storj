// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package ecclient

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"sort"
	"time"

	"go.uber.org/zap"
	monkit "gopkg.in/spacemonkeygo/monkit.v2"

	"storj.io/storj/pkg/eestream"
	"storj.io/storj/pkg/pb"
	"storj.io/storj/pkg/piecestore/rpc/client"
	"storj.io/storj/pkg/provider"
	"storj.io/storj/pkg/ranger"
	"storj.io/storj/pkg/transport"
	"storj.io/storj/pkg/utils"
)

var mon = monkit.Package()

// Client defines an interface for storing erasure coded data to piece store nodes
type Client interface {
	Repair(ctx context.Context, healthyNodes []*pb.Node, newNodes []*pb.Node, rs eestream.RedundancyStrategy,
		pieceID client.PieceID, data io.Reader, expiration time.Time) (successfulNodes []*pb.Node, err error)
	Put(ctx context.Context, nodes []*pb.Node, rs eestream.RedundancyStrategy,
		pieceID client.PieceID, data io.Reader, expiration time.Time) (successfulNodes []*pb.Node, err error)
	Get(ctx context.Context, nodes []*pb.Node, es eestream.ErasureScheme,
		pieceID client.PieceID, size int64) (ranger.Ranger, error)
	Delete(ctx context.Context, nodes []*pb.Node, pieceID client.PieceID) error
}

type dialer interface {
	dial(ctx context.Context, node *pb.Node) (ps client.PSClient, err error)
}

type defaultDialer struct {
	transport transport.Client
	identity  *provider.FullIdentity
}

func (dialer *defaultDialer) dial(ctx context.Context, node *pb.Node) (ps client.PSClient, err error) {
	defer mon.Task()(&ctx)(&err)

	conn, err := dialer.transport.DialNode(ctx, node)
	if err != nil {
		return nil, err
	}

	return client.NewPSClient(conn, 0, dialer.identity.Key)
}

type ecClient struct {
	d   dialer
	mbm int
}

// NewClient from the given TransportClient and max buffer memory
func NewClient(identity *provider.FullIdentity, transport transport.Client, mbm int) Client {
	d := defaultDialer{identity: identity, transport: transport}
	return &ecClient{d: &d, mbm: mbm}
}

func (ec *ecClient) Repair(ctx context.Context, healthyNodes []*pb.Node, newNodes []*pb.Node, rs eestream.RedundancyStrategy,
	pieceID client.PieceID, data io.Reader, expiration time.Time) (successfulNodes []*pb.Node, err error) {
	defer mon.Task()(&ctx)(&err)

	padded := eestream.PadReader(ioutil.NopCloser(data), rs.StripeSize())
	readers, err := eestream.EncodeReader(ctx, padded, rs, ec.mbm)
	if err != nil {
		return nil, err
	}

	type info struct {
		i   int
		err error
	}
	infos := make(chan info, len(healthyNodes))
	newIds := len(newNodes)
	for i, n := range healthyNodes {
		log.Println("KISHORE --> value of i =", i)
		log.Println("KISHORE --> value of n =", n)
		log.Println("KISHORE --> value of healtyNodes[", i, "] =", healthyNodes[i])

		if healthyNodes[i] != nil {
			log.Println("KISHORE --> Inside erasure client i =", i)
			infos <- info{i: i, err: nil}
			continue
		}
		newIds = newIds - 1
		healthyNodes[i] = newNodes[newIds]
		n = healthyNodes[i]
		log.Println("KISHORE --> value of newIds =", newIds)
		log.Println("KISHORE --> outside erasure client i =", i, "healtyNodes[", i, "]=", healthyNodes[i])
		go func(i int, n *pb.Node) {
			derivedPieceID, err := pieceID.Derive([]byte(n.GetId()))
			if err != nil {
				zap.S().Errorf("Failed deriving piece id for %s: %v", pieceID, err)
				infos <- info{i: i, err: err}
				return
			}
			ps, err := ec.d.dial(ctx, n)
			if err != nil {
				zap.S().Errorf("Failed putting piece %s -> %s to node %s: %v",
					pieceID, derivedPieceID, n.GetId(), err)
				infos <- info{i: i, err: err}
				return
			}
			err = ps.Put(ctx, derivedPieceID, readers[i], expiration, &pb.PayerBandwidthAllocation{})
			// normally the bellow call should be deferred, but doing so fails
			// randomly the unit tests
			utils.LogClose(ps)
			// io.ErrUnexpectedEOF means the piece upload was interrupted due to slow connection.
			// No error logging for this case.
			if err != nil && err != io.ErrUnexpectedEOF {
				zap.S().Errorf("Failed putting piece %s -> %s to node %s: %v",
					pieceID, derivedPieceID, n.GetId(), err)
			}
			infos <- info{i: i, err: err}
		}(i, n)
	}

	successfulNodes = make([]*pb.Node, len(healthyNodes))
	var successfulCount int
	for range healthyNodes {
		info := <-infos
		if info.err == nil {
			successfulNodes[info.i] = healthyNodes[info.i]
			successfulCount++
		}
	}
	log.Println("KISHORE --> successfulCount=", successfulCount, "successfulNodes =", successfulNodes)

	return successfulNodes, nil
}

func (ec *ecClient) Put(ctx context.Context, nodes []*pb.Node, rs eestream.RedundancyStrategy,
	pieceID client.PieceID, data io.Reader, expiration time.Time) (successfulNodes []*pb.Node, err error) {
	defer mon.Task()(&ctx)(&err)

	if len(nodes) != rs.TotalCount() {
		return nil, Error.New("number of nodes (%d) do not match total count (%d) of erasure scheme", len(nodes), rs.TotalCount())
	}
	if !unique(nodes) {
		return nil, Error.New("duplicated nodes are not allowed")
	}

	padded := eestream.PadReader(ioutil.NopCloser(data), rs.StripeSize())
	readers, err := eestream.EncodeReader(ctx, padded, rs, ec.mbm)
	if err != nil {
		return nil, err
	}

	type info struct {
		i   int
		err error
	}
	infos := make(chan info, len(nodes))

	for i, n := range nodes {

		go func(i int, n *pb.Node) {
			if n == nil {
				_, err := io.Copy(ioutil.Discard, readers[i])
				infos <- info{i: i, err: err}
				return
			}
			derivedPieceID, err := pieceID.Derive([]byte(n.GetId()))

			if err != nil {
				zap.S().Errorf("Failed deriving piece id for %s: %v", pieceID, err)
				infos <- info{i: i, err: err}
				return
			}
			ps, err := ec.d.dial(ctx, n)
			if err != nil {
				zap.S().Errorf("Failed putting piece %s -> %s to node %s: %v",
					pieceID, derivedPieceID, n.GetId(), err)
				infos <- info{i: i, err: err}
				return
			}

			err = ps.Put(ctx, derivedPieceID, readers[i], expiration, &pb.PayerBandwidthAllocation{})
			// normally the bellow call should be deferred, but doing so fails
			// randomly the unit tests
			utils.LogClose(ps)
			// io.ErrUnexpectedEOF means the piece upload was interrupted due to slow connection.
			// No error logging for this case.
			if err != nil && err != io.ErrUnexpectedEOF {
				zap.S().Errorf("Failed putting piece %s -> %s to node %s: %v",
					pieceID, derivedPieceID, n.GetId(), err)
			}
			infos <- info{i: i, err: err}
		}(i, n)
	}

	successfulNodes = make([]*pb.Node, len(nodes))
	var successfulCount int
	for range nodes {
		info := <-infos
		if info.err == nil {
			successfulNodes[info.i] = nodes[info.i]
			successfulCount++
		}
	}

	/* clean up the partially uploaded segment's pieces */
	defer func() {
		select {
		case <-ctx.Done():
			err = utils.CombineErrors(
				Error.New("upload cancelled by user"),
				ec.Delete(context.Background(), nodes, pieceID),
			)
		default:
		}
	}()

	if successfulCount < rs.RepairThreshold() {
		return nil, Error.New("successful puts (%d) less than repair threshold (%d)", successfulCount, rs.RepairThreshold())
	}

	return successfulNodes, nil
}

func (ec *ecClient) Get(ctx context.Context, nodes []*pb.Node, es eestream.ErasureScheme,
	pieceID client.PieceID, size int64) (rr ranger.Ranger, err error) {
	defer mon.Task()(&ctx)(&err)

	if len(nodes) != es.TotalCount() {
		return nil, Error.New("number of nodes (%v) do not match total count (%v) of erasure scheme", len(nodes), es.TotalCount())
	}

	paddedSize := calcPadded(size, es.StripeSize())
	pieceSize := paddedSize / int64(es.RequiredCount())
	rrs := map[int]ranger.Ranger{}

	type rangerInfo struct {
		i   int
		rr  ranger.Ranger
		err error
	}
	ch := make(chan rangerInfo, len(nodes))

	for i, n := range nodes {
		if n == nil {
			ch <- rangerInfo{i: i, rr: nil, err: nil}
			continue
		}

		go func(i int, n *pb.Node) {
			derivedPieceID, err := pieceID.Derive([]byte(n.GetId()))
			if err != nil {
				zap.S().Errorf("Failed deriving piece id for %s: %v", pieceID, err)
				ch <- rangerInfo{i: i, rr: nil, err: err}
				return
			}

			rr := &lazyPieceRanger{
				dialer: ec.d,
				node:   n,
				id:     derivedPieceID,
				size:   pieceSize,
				pba:    &pb.PayerBandwidthAllocation{},
			}

			ch <- rangerInfo{i: i, rr: rr, err: nil}
		}(i, n)
	}

	for range nodes {
		rri := <-ch
		if rri.err == nil && rri.rr != nil {
			rrs[rri.i] = rri.rr
		}
	}

	rr, err = eestream.Decode(rrs, es, ec.mbm)
	if err != nil {
		return nil, err
	}

	return eestream.Unpad(rr, int(paddedSize-size))
}

func (ec *ecClient) Delete(ctx context.Context, nodes []*pb.Node, pieceID client.PieceID) (err error) {
	defer mon.Task()(&ctx)(&err)

	errs := make(chan error, len(nodes))

	for _, n := range nodes {
		if n == nil {
			errs <- nil
			continue
		}

		go func(n *pb.Node) {
			derivedPieceID, err := pieceID.Derive([]byte(n.GetId()))
			if err != nil {
				zap.S().Errorf("Failed deriving piece id for %s: %v", pieceID, err)
				errs <- err
				return
			}
			ps, err := ec.d.dial(ctx, n)
			if err != nil {
				zap.S().Errorf("Failed deleting piece %s -> %s from node %s: %v",
					pieceID, derivedPieceID, n.GetId(), err)
				errs <- err
				return
			}
			err = ps.Delete(ctx, derivedPieceID)
			// normally the bellow call should be deferred, but doing so fails
			// randomly the unit tests
			utils.LogClose(ps)
			if err != nil {
				zap.S().Errorf("Failed deleting piece %s -> %s from node %s: %v",
					pieceID, derivedPieceID, n.GetId(), err)
			}
			errs <- err
		}(n)
	}

	allerrs := collectErrors(errs, len(nodes))

	if len(allerrs) > 0 && len(allerrs) == len(nodes) {
		return allerrs[0]
	}

	return nil
}

func collectErrors(errs <-chan error, size int) []error {
	var result []error
	for i := 0; i < size; i++ {
		err := <-errs
		if err != nil {
			result = append(result, err)
		}
	}
	return result
}

func unique(nodes []*pb.Node) bool {
	if len(nodes) < 2 {
		return true
	}

	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.GetId()
	}

	// sort the ids and check for identical neighbors
	sort.Strings(ids)
	for i := 1; i < len(ids); i++ {
		if ids[i] != "" && ids[i] == ids[i-1] {
			return false
		}
	}

	return true
}

func calcPadded(size int64, blockSize int) int64 {
	mod := size % int64(blockSize)
	if mod == 0 {
		return size
	}
	return size + int64(blockSize) - mod
}

type lazyPieceRanger struct {
	ranger ranger.Ranger
	dialer dialer
	node   *pb.Node
	id     client.PieceID
	size   int64
	pba    *pb.PayerBandwidthAllocation
}

// Size implements Ranger.Size
func (lr *lazyPieceRanger) Size() int64 {
	return lr.size
}

// Range implements Ranger.Range to be lazily connected
func (lr *lazyPieceRanger) Range(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	if lr.ranger == nil {
		ps, err := lr.dialer.dial(ctx, lr.node)
		if err != nil {
			return nil, err
		}
		ranger, err := ps.Get(ctx, lr.id, lr.size, lr.pba)
		if err != nil {
			return nil, err
		}
		lr.ranger = ranger
	}
	return lr.ranger.Range(ctx, offset, length)
}
