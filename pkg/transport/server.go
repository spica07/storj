// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package transport

import (
	"net"

	"google.golang.org/grpc"

	"storj.io/storj/pkg/provider"
)

type Server struct {
	listener net.Listener
	identity *provider.FullIdentity
	grpc     *grpc.Server
}

// NewServer creates a server with default transport and settings
func NewServer(identity *provider.FullIdentity, address string) (*Server, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	options, err := identity.ServerOption()
	if err != nil {
		return nil, err
	}

	server := grpc.NewServer(options)
	return &Server{
		listener: listener,
		identity: identity,
		grpc:     server,
	}, nil
}

// NewTestServer creates a new test server with an in-memory identity
func NewTestServer() (*Server, error) {
	ca, err := provider.NewCA(ctx, 12, 4)
	if err != nil {
		return nil, err
	}

	identity, err := ca.NewIdentity()
	if err != nil {
		return nil, err
	}

	return NewServer(identity, "127.0.0.1:0")
}

// ID returns the identity this server has
// TODO: hide
func (server *Server) ID() string {
	return server.identity.ID.String()
}

// Identity returns the identity this server has
// TODO: hide
func (server *Server) Identity() *provider.FullIdentity {
	return server.identity
}

// GRPC returns the grpc server for registration
func (server *Server) GRPC() *grpc.Server {
	return server.grpc
}

// Addr returns the address of the server
func (server *Server) Addr() string {
	return server.listener.Addr().String()
}

// Serve starts serving on the address
func (server *Server) Serve() error {
	return server.grpc.Serve(server.listener)
}

// Close shuts down the server
func (server *Server) Close() error {
	server.grpc.GracefulStop()
	return nil
}
