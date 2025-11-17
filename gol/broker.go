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

// Run implements a simple "sub-world" style distributed execution.
// For each turn the broker owns the full board, splits it into horizontal
// chunks, sends each chunk to a worker along with its halo rows, and then
// reconstructs the global board from the results returned by the workers.
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
			return fmt.Errorf("failed to connect to worker %s: %v", addr, err)
		}
		clients[i] = cli
	}
	b.clients = clients

	height := len(board)
	if height == 0 {
		return fmt.Errorf("empty board")
	}
	width := len(board[0])

	for turn := 0; turn < p.Turns; turn++ {
		// Allocate a new board to store the updated state for this turn.
		newBoard := make([][]uint8, height)
		for y := range newBoard {
			newBoard[y] = make([]uint8, width)
		}

		base := height / len(clients)
		rem := height % len(clients)
		start := 0

		for i, cli := range clients {
			h := base
			if i < rem {
				h++
			}

			// Build the chunk for this worker by copying its rows.
			rows := make([][]uint8, h)
			for r := 0; r < h; r++ {
				row := make([]uint8, width)
				copy(row, board[start+r])
				rows[r] = row
			}

			chunk := Chunk{
				Width:  width,
				Height: h,
				Rows:   rows,
			}

			initArgs := InitArgs{Chunk: chunk, IsToroidal: true}
			var initRep InitReply
			if err := cli.Call("Worker.Init", &initArgs, &initRep); err != nil || !initRep.Ok {
				return fmt.Errorf("init worker %d failed: %v", i, err)
			}

			// Compute halo rows for this chunk from the global board.
			topIndex := (start - 1 + height) % height
			bottomIndex := (start + h) % height

			haloTop := make([]uint8, width)
			haloBottom := make([]uint8, width)
			copy(haloTop, board[topIndex])
			copy(haloBottom, board[bottomIndex])

			stepArgs := StepArgs{HaloTop: haloTop, HaloBottom: haloBottom}
			var stepRep StepReply
			if err := cli.Call("Worker.Step", &stepArgs, &stepRep); err != nil || !stepRep.Ok {
				return fmt.Errorf("step worker %d failed: %v", i, err)
			}

			var getRep GetReply
			if err := cli.Call("Worker.Get", &GetArgs{}, &getRep); err != nil {
				return fmt.Errorf("get worker %d failed: %v", i, err)
			}

			if len(getRep.Chunk.Rows) != h {
				return fmt.Errorf("worker %d returned chunk with wrong height", i)
			}
			for r := 0; r < h; r++ {
				copy(newBoard[start+r], getRep.Chunk.Rows[r])
			}

			start += h
		}

		board = newBoard

		b.mu.Lock()
		b.current = board
		b.turns = turn + 1
		b.mu.Unlock()
	}

	reply.FinalBoard = board
	reply.AliveCount = countAlive(board)
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
