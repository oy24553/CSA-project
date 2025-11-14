package gol

import (
	"fmt"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels, control <-chan rune) {

	c.ioCommand <- ioInput
	c.ioFilename <- fmt.Sprintf("%vx%v", p.ImageWidth, p.ImageHeight)

	board := MakeBoard(p.ImageHeight, p.ImageWidth)
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			board[y][x] = <-c.ioInput
		}
	}

	brokerAddr := os.Getenv("BROKER_ADDR")
	if brokerAddr == "" {
		brokerAddr = "localhost:8040"
	}
	client, err := rpc.Dial("tcp", brokerAddr)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Broker at %v: %v", brokerAddr, err))
	}
	defer client.Close()

	var initCells []util.Cell
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if board[y][x] == 255 {
				initCells = append(initCells, util.Cell{X: x, Y: y})
			}
		}
	}
	c.events <- CellsFlipped{
		CompletedTurns: 0,
		Cells:          initCells,
	}
	c.events <- TurnComplete{CompletedTurns: 0}
	c.events <- StateChange{0, Executing}

	var latestCompleted int64
	var latestAlive int64
	atomic.StoreInt64(&latestAlive, int64(countAlive(board)))

	stopProgress := make(chan struct{})
	var statsWG sync.WaitGroup
	statsWG.Add(1)
	go func() {
		defer statsWG.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		lastReported := int64(-1)
		for {
			select {
			case <-ticker.C:
				curr := atomic.LoadInt64(&latestCompleted)
				if curr >= 0 && curr != lastReported {
					alive := int(atomic.LoadInt64(&latestAlive))
					c.events <- AliveCellsCount{CompletedTurns: int(curr), CellsCount: alive}
					lastReported = curr
				}
			case <-stopProgress:
				return
			}
		}
	}()

	const chunkTurns = 64

	paused := false
	quit := false
	shutdown := false
	completed := 0
	aliveCount := 0

	for !quit && !shutdown && completed < p.Turns {
		select {
		case key := <-control:
			switch key {
			case 's':
				filename := fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, completed)
				saveBoard(c, board, filename, p, completed)
			case 'q':
				filename := fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, completed)
				saveBoard(c, board, filename, p, completed)
				c.events <- StateChange{completed, Quitting}
				quit = true
			case 'k':
				filename := fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, completed)
				saveBoard(c, board, filename, p, completed)
				c.events <- StateChange{completed, Quitting}
				shutdown = true
			case 'p':
				paused = !paused
				eventTurns := clampPauseTurns(completed)
				if paused {
					c.events <- StateChange{eventTurns, Paused}
				} else {
					c.events <- StateChange{eventTurns, Executing}
				}
			}

		default:
			if paused {
				time.Sleep(200 * time.Millisecond)
				continue
			}

			remaining := p.Turns - completed
			turns := chunkTurns
			if remaining < turns {
				turns = remaining
			}

			chunkParams := p
			chunkParams.Turns = turns

			args := RunArgs{Params: chunkParams, Board: board}
			var rep RunReply
			if err := client.Call("Broker.Run", args, &rep); err != nil {
				panic(fmt.Sprintf("Broker.Run() failed: %v", err))
			}

			board = rep.FinalBoard
			completed += turns
			aliveCount = rep.AliveCount

			var frameCells []util.Cell
			for y := 0; y < p.ImageHeight; y++ {
				for x := 0; x < p.ImageWidth; x++ {
					if board[y][x] == 255 {
						frameCells = append(frameCells, util.Cell{X: x, Y: y})
					}
				}
			}

			c.events <- CellsFlipped{CompletedTurns: completed, Cells: frameCells}
			c.events <- TurnComplete{CompletedTurns: completed}

			atomic.StoreInt64(&latestCompleted, int64(completed))
			atomic.StoreInt64(&latestAlive, int64(aliveCount))
		}
	}

	close(stopProgress)
	statsWG.Wait()

	finalName := fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, p.Turns)
	c.ioCommand <- ioOutput
	c.ioFilename <- finalName
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.ioOutput <- board[y][x]
		}
	}
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	c.events <- ImageOutputComplete{
		CompletedTurns: p.Turns,
		Filename:       finalName,
	}

	var aliveCells []util.Cell
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if board[y][x] == 255 {
				aliveCells = append(aliveCells, util.Cell{X: x, Y: y})
			}
		}
	}

	c.events <- FinalTurnComplete{
		CompletedTurns: p.Turns,
		Alive:          aliveCells,
	}

	c.events <- StateChange{p.Turns, Quitting}

	close(c.events)

	if shutdown {
		os.Exit(0)
	}
}

func saveBoard(c distributorChannels, board [][]uint8, name string, p Params, completed int) {
	c.ioCommand <- ioOutput
	c.ioFilename <- name
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.ioOutput <- board[y][x]
		}
	}
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	c.events <- ImageOutputComplete{
		CompletedTurns: completed,
		Filename:       name,
	}
}

//const defaultWorkerAddress = "localhost:8030"

func splitBoardRows(board [][]uint8, n int) []Chunk {
	H := len(board)
	if H == 0 {
		return nil
	}
	W := len(board[0])
	base := H / n
	rem := H % n

	chunks := make([]Chunk, n)
	start := 0
	for i := 0; i < n; i++ {
		h := base
		if i < rem {
			h++
		}
		rows := make([][]uint8, h)
		for r := 0; r < h; r++ {
			row := make([]uint8, W)
			copy(row, board[start+r])
			rows[r] = row
		}
		chunks[i] = Chunk{Width: W, Height: h, Rows: rows}
		start += h
	}
	return chunks
}

func MakeBoard(height, width int) [][]uint8 {
	board := make([][]uint8, height)
	for i := range board {
		board[i] = make([]uint8, width)
	}
	return board
}

func countAlive(board [][]uint8) int {
	total := 0
	for y := range board {
		for x := range board[y] {
			if board[y][x] == 255 {
				total++
			}
		}
	}
	return total
}

func fetchAndMerge(clients []*rpc.Client) [][]uint8 {
	results := make([][][]uint8, len(clients))
	var wg sync.WaitGroup
	wg.Add(len(clients))
	for i, cli := range clients {
		go func(i int, cli *rpc.Client) {
			defer wg.Done()
			var rep GetReply
			if err := cli.Call("Worker.Get", GetArgs{}, &rep); err == nil {
				results[i] = rep.Chunk.Rows
			}
		}(i, cli)
	}
	wg.Wait()

	merged := make([][]uint8, 0)
	for i := range results {
		merged = append(merged, results[i]...)
	}
	return merged
}

func clampPauseTurns(completed int) int {
	if completed <= 0 {
		return 0
	}
	return 1
}

/*func countAliveRemote(clients []*rpc.Client) int {
	total := int64(0)
	var wg sync.WaitGroup
	wg.Add(len(clients))
	for _, cli := range clients {
		go func(cli *rpc.Client) {
			defer wg.Done()
			var args AliveArgs
			var rep AliveReply
			if err := cli.Call("Worker.Alive", &args, &rep); err == nil {
				atomic.AddInt64(&total, int64(rep.Count))
			}
		}(cli)
	}
	wg.Wait()
	return int(total)
}*/
