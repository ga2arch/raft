package main

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"raft/raft"
	"time"
)


func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	node := raft.NewFollowerNode()
	rpcConfig := &raft.RpcConfig{NodeChan: node.MessageChan}

	// starting rpc server
	err := rpc.Register(rpcConfig)
	if err != nil {
		panic(err)
	}

	l, e := net.Listen("tcp", os.Args[1])
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		for {
			conn, _ := l.Accept()
			go rpc.ServeConn(conn)
		}
	}()


	// iterate Peers to connect from arguments
	for i := 2; i < len(os.Args); i++ {
		addr := os.Args[i]

		// connect to peer
		log.Printf("dialing %s", addr)
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			log.Printf("cannot dial %s", addr)
			continue
		}
		log.Printf("connected to %s", addr)

		var reply raft.HandshakeResponse
		err = client.Call("RpcConfig.Handshake", &raft.HandshakeCmd{Addr: "127.0.0.1" + os.Args[1]}, &reply)
		if err != nil {
			log.Printf("failed handshake with %s, err: %s", addr, err)
		}

		// create peer
		peer := &raft.Peer{Addr: os.Args[i], Client: client}
		node.AddPeer(peer)
	}

	log.Printf("connected to %d Peers", len(node.Peers))
	go func() {
		time.Sleep(10 * time.Second)
		node.Send(raft.Message{Payload:raft.AddLogCmd{Command:"SET KEK TOP"}})
	}()
	node.Run()
}