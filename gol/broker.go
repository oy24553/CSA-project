package gol

import (
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"sync"
)

type Broker struct {
	clients []*rpc.Client
	mu      sync.Mutex
	current [][]uint8
	turns   int
}

type RunArgs struct {
	Params Params
	Board  [][]uint8
}

type RunReply struct {
	FinalBoard [][]uint8
	AliveCount int
	Turns      int
}

func (b *Broker) Run(args RunArgs, reply *RunReply) error {
	p := args.Params
	board := args.Board

	workersEnv := os.Getenv("GOL_WORKERS")
	if workersEnv == "" {
		workersEnv = "localhost:8030"
	}
	addrs := strings.Split(workersEnv, ",")
	clients := make([]*rpc.Client, len(addrs))
	for i, addr := range addrs {
		cli, err := rpc.Dial("tcp", addr)
		if err != nil {
			panic(fmt.Sprintf("Failed to connect to worker %s: %v", addr, err))
		}
		clients[i] = cli
	}
	b.clients = clients

	chunks := splitBoardRows(board, len(clients))
	for i, cli := range clients {
		args := InitArgs{Chunk: chunks[i], IsToroidal: true}
		var rep InitReply
		if err := cli.Call("Worker.Init", &args, &rep); err != nil || !rep.Ok {
			panic(fmt.Sprintf("Init worker %d failed: %v", i, err))
		}
	}

	board = fetchAndMerge(clients)
	b.mu.Lock()
	b.current = board
	b.turns = 0
	b.mu.Unlock()

	const chunkTurns = 64
	completed := 0
	for completed < p.Turns {
		remaining := p.Turns - completed
		turns := chunkTurns
		if remaining < turns {
			turns = remaining
		}

		for t := 0; t < turns; t++ {
			topRows := make([][]uint8, len(clients))
			bottomRows := make([][]uint8, len(clients))

			var wgGE sync.WaitGroup
			wgGE.Add(len(clients))
			for i, cli := range clients {
				go func(i int, cli *rpc.Client) {
					defer wgGE.Done()
					var rep EdgesReply
					if err := cli.Call("Worker.GetEdges", EdgesArgs{}, &rep); err == nil {
						topRows[i], bottomRows[i] = rep.Top, rep.Bottom
					}
				}(i, cli)
			}
			wgGE.Wait()

			var wgSt sync.WaitGroup
			wgSt.Add(len(clients))
			for i, cli := range clients {
				go func(i int, cli *rpc.Client) {
					defer wgSt.Done()
					haloTop := bottomRows[(i-1+len(clients))%len(clients)]
					haloBottom := topRows[(i+1)%len(clients)]
					args := StepArgs{HaloTop: haloTop, HaloBottom: haloBottom}
					var rep StepReply
					cli.Call("Worker.Step", args, &rep)
				}(i, cli)
			}
			wgSt.Wait()
		}
		completed += turns
		cur := fetchAndMerge(clients)

		b.mu.Lock()
		b.current = cur
		b.turns = completed
		b.mu.Unlock()
	}

	finalBoard := fetchAndMerge(clients)
	reply.FinalBoard = finalBoard
	reply.AliveCount = countAlive(finalBoard)
	reply.Turns = p.Turns
	return nil

}

func (b *Broker) GetProgress(_ struct{}, reply *RunReply) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.clients) == 0 || b.current == nil {
		return fmt.Errorf("[Broker] No progress yet")
	}
	reply.FinalBoard = b.current
	reply.AliveCount = countAlive(b.current)
	reply.Turns = b.turns
	return nil
}
