package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/tonutils-go/ton/wallet"
)

type candidate struct {
	score      int
	walletType string
	seed       []string
	address    string
}

var found int32
var bestCandidate candidate
var bestMu sync.Mutex
var foundWallet struct {
	walletType string
	seed       []string
	address    string
}
var mu sync.Mutex
var lastLog atomic.Value

func computeScore(cSuffix, target string) int {
	score := 0
	for i := 0; i < len(target); i++ {
		if strings.ToLower(string(cSuffix[i])) != strings.ToLower(string(target[i])) {
			score++
		}
	}
	return score
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Впиши последние символы адреса: ")
	suffix, _ := reader.ReadString('\n')
	suffix = strings.TrimSpace(suffix)
	bestCandidate = candidate{score: len(suffix) + 1}

	availableThreads := runtime.NumCPU()
	if osThreadsRaw := strings.TrimSpace(os.Getenv("NUMBER_OF_PROCESSORS")); osThreadsRaw != "" {
		if osThreads, err := strconv.Atoi(osThreadsRaw); err == nil && osThreads > 0 {
			availableThreads = osThreads
		}
	}

	fmt.Printf("Доступно потоков на компе (из ОС): %d\n", availableThreads)
	fmt.Printf("Сколько потоков использовать (1-%d, Enter = %d): ", availableThreads, availableThreads)
	threadInput, _ := reader.ReadString('\n')
	threadInput = strings.TrimSpace(threadInput)

	threadCount := availableThreads
	if threadInput != "" {
		if parsed, err := strconv.Atoi(threadInput); err == nil {
			switch {
			case parsed < 1:
				threadCount = 1
			case parsed > availableThreads:
				threadCount = availableThreads
			default:
				threadCount = parsed
			}
		} else {
			log.Printf("Некорректное число потоков, использую значение по умолчанию: %d", threadCount)
		}
	}

	log.Printf("Использую %d потоков\n", threadCount)
	var wg sync.WaitGroup
	doneLog := make(chan struct{})
	go func() {
		var lastPrinted string
		for {
			select {
			case <-doneLog:
				return
			default:
				msg, _ := lastLog.Load().(string)
				if msg != "" && msg != lastPrinted {
					fmt.Println(msg)
					lastPrinted = msg
				}
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		atomic.StoreInt32(&found, 1)
		close(doneLog)
	}()
	for i := 0; i < threadCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if err := recover(); err != nil {
					log.Printf("Recovered panic: %v", err)
					debug.Stack()
				}
			}()
			for atomic.LoadInt32(&found) == 0 {
				seed := wallet.NewSeed()
				w, err := wallet.FromSeed(nil, seed, wallet.ConfigV5R1Final{
					NetworkGlobalID: -239,
					Workchain:       0,
				})
				if err == nil {
					addr := w.WalletAddress().Testnet(false).Bounce(false).String()
					if len(addr) >= len(suffix) {
						cSuffix := addr[len(addr)-len(suffix):]
						logPart := fmt.Sprintf("LOG: V5: %s", cSuffix)
						lastLog.Store(logPart)
						if cSuffix == suffix {
							mu.Lock()
							if atomic.LoadInt32(&found) == 0 {
								foundWallet.walletType = "V5"
								foundWallet.seed = append([]string{}, seed...)
								foundWallet.address = addr
								atomic.StoreInt32(&found, 1)
							}
							mu.Unlock()
							return
						}
						score := computeScore(cSuffix, suffix)
						bestMu.Lock()
						if score < bestCandidate.score {
							bestCandidate.score = score
							bestCandidate.walletType = "V5"
							bestCandidate.seed = append([]string{}, seed...)
							bestCandidate.address = addr
						}
						bestMu.Unlock()
					}
				}
			}
		}()
	}
	wg.Wait()
	if foundWallet.address != "" {
		fmt.Println("=== NICKXD ===")
		fmt.Println("Тип:", foundWallet.walletType)
		fmt.Println("Seed phrase:", strings.Join(foundWallet.seed, " "))
		fmt.Println("Address:", foundWallet.address)
	} else if bestCandidate.address != "" {
		fmt.Println("=== SAMY BLIZKI ===")
		fmt.Println("Тип:", bestCandidate.walletType)
		fmt.Println("Seed:", strings.Join(bestCandidate.seed, " "))
		fmt.Println("Address:", bestCandidate.address)
	} else {
		fmt.Println("No close wallet found")
	}
	time.Sleep(100000000 * time.Second)
}
