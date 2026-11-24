// Command mdt-replay sends recorded Cisco MDT messages to a pipeline, acting
// like an IOS XR device configured for dial-out. Use it to test a deployment
// without a router:
//
//	mdt-replay -addr localhost:57500 internal/input/ciscomdt/testdata/*.kvgpb
//	mdt-replay -transport tcp -addr localhost:5432 -repeat 10 -interval 1s capture.kvgpb
//
// Each file holds one serialised Telemetry message (KV-GPB) or one JSON
// message.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	mdpb "github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/gen/cisco/mdtdialout"
	"github.com/sangmeshcp/bigmuddy-network-telemetry-pipeline/internal/input/ciscomdt"
)

func main() {
	addr := flag.String("addr", "localhost:57500", "pipeline dial-out address")
	transport := flag.String("transport", "grpc", "grpc or tcp")
	useTLS := flag.Bool("tls", false, "use TLS (server certificate not verified)")
	repeat := flag.Int("repeat", 1, "times to send the file set")
	interval := flag.Duration("interval", 0, "pause between rounds")
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: mdt-replay [flags] file...")
		os.Exit(2)
	}
	var payloads [][]byte
	for _, f := range flag.Args() {
		b, err := os.ReadFile(f)
		if err != nil {
			fail(err)
		}
		payloads = append(payloads, b)
	}
	var err error
	switch *transport {
	case "grpc":
		err = sendGRPC(*addr, *useTLS, payloads, *repeat, *interval)
	case "tcp":
		err = sendTCP(*addr, *useTLS, payloads, *repeat, *interval)
	default:
		err = fmt.Errorf("unknown transport %q", *transport)
	}
	if err != nil {
		fail(err)
	}
	fmt.Printf("sent %d messages\n", len(payloads)**repeat)
}

func sendGRPC(addr string, useTLS bool, payloads [][]byte, repeat int, interval time.Duration) error {
	creds := insecure.NewCredentials()
	if useTLS {
		creds = credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // test tool
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second+time.Duration(repeat)*interval)
	defer cancel()
	stream, err := mdpb.NewGRPCMdtDialoutClient(conn).MdtDialout(ctx)
	if err != nil {
		return err
	}
	var id int64
	for r := 0; r < repeat; r++ {
		for _, p := range payloads {
			id++
			if err := stream.Send(&mdpb.MdtDialoutArgs{ReqId: id, Data: p}); err != nil {
				return err
			}
		}
		time.Sleep(interval)
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	_, err = stream.Recv() // wait for the server to finish reading
	if err != nil && !strings.Contains(err.Error(), "EOF") {
		return err
	}
	return nil
}

func sendTCP(addr string, useTLS bool, payloads [][]byte, repeat int, interval time.Duration) error {
	var conn net.Conn
	var err error
	if useTLS {
		conn, err = tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // test tool
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		return err
	}
	defer conn.Close()
	for r := 0; r < repeat; r++ {
		for _, p := range payloads {
			json := len(p) > 0 && p[0] == '{'
			if _, err := conn.Write(ciscomdt.EncodeST(p, json)); err != nil {
				return err
			}
		}
		time.Sleep(interval)
	}
	return nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "mdt-replay:", err)
	os.Exit(1)
}
