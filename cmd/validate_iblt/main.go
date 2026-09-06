package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hackathon/sync-engine/pkg/recon"
)

func main() {
	deltas := []int{0, 1, 5, 10, 20, 50, 100, 500, 1000}
	const totalItems = 5000
	const trials = 10

	fmt.Println("=========================================================================================================")
	fmt.Println("                                      IBLT SET RECONCILIATION BENCHMARK                                  ")
	fmt.Println("=========================================================================================================")

	_ = os.MkdirAll("bench/results", 0755)
	csvFile, err := os.Create("bench/results/iblt_validation.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	writer.Write([]string{
		"DeltaItems",
		"FixedIBLTBytes",
		"FixedDecodeSuccessRate",
		"AdaptiveIBLTBytes",
		"AdaptiveDecodeSuccessRate",
		"ReconTimeMicros",
		"FallbackEngaged",
	})

	fmt.Printf("%-12s | %-15s | %-12s | %-16s | %-12s | %-12s | %-10s\n",
		"Delta (Δ)", "Fixed 30-Bucket", "Fixed Succ%", "Adaptive Sized", "Adapt Succ%", "Recon Latency", "Fallback?")
	fmt.Println("---------------------------------------------------------------------------------------------------------")

	for _, d := range deltas {
		fixedSucc := 0
		adaptSucc := 0
		var totalReconTime time.Duration
		var fixedBytes int
		var adaptBytes int

		for t := 0; t < trials; t++ {
			// Generate sets
			setA := make([]uint64, totalItems)
			setB := make([]uint64, totalItems)
			baseOffset := uint64(t*1000000 + 1)
			for i := 0; i < totalItems; i++ {
				setA[i] = baseOffset + uint64(i)
				if i < totalItems-d {
					setB[i] = baseOffset + uint64(i)
				} else {
					setB[i] = baseOffset + uint64(i) + 5000000 // changed item
				}
			}

			// 1. Fixed default table (30 buckets)
			fixedTable := recon.BuildTable(setA)
			fixedBytes = len(fixedTable.ToBytes())
			diffFixed, errF := recon.Compare(setB, fixedTable.ToBytes())
			if errF == nil && diffFixed.Success && len(diffFixed.Missing) == d {
				fixedSucc++
			}

			// 2. Adaptive table (with capacity d)
			t0 := time.Now()
			adaptTable := recon.BuildTableWithCapacity(setA, d)
			adaptBytes = len(adaptTable.ToBytes())
			diffAdapt, errA := recon.Compare(setB, adaptTable.ToBytes())
			elapsed := time.Since(t0)
			totalReconTime += elapsed

			if errA == nil && diffAdapt.Success && len(diffAdapt.Missing) == d {
				adaptSucc++
			}
		}

		fixedPct := float64(fixedSucc) / float64(trials) * 100.0
		adaptPct := float64(adaptSucc) / float64(trials) * 100.0
		avgMicros := (totalReconTime / time.Duration(trials)).Microseconds()
		fallback := "NO"
		if adaptPct < 100.0 || fixedPct < 100.0 {
			fallback = "YES (safeguard)"
		}

		fmt.Printf("%-12d | %-15s | %-11.1f%% | %-16s | %-11.1f%% | %-10dµs | %-10s\n",
			d,
			fmt.Sprintf("%d B", fixedBytes),
			fixedPct,
			fmt.Sprintf("%d B", adaptBytes),
			adaptPct,
			avgMicros,
			fallback,
		)

		writer.Write([]string{
			fmt.Sprintf("%d", d),
			fmt.Sprintf("%d", fixedBytes),
			fmt.Sprintf("%.1f", fixedPct),
			fmt.Sprintf("%d", adaptBytes),
			fmt.Sprintf("%.1f", adaptPct),
			fmt.Sprintf("%d", avgMicros),
			fallback,
		})
	}
	fmt.Println("=========================================================================================================\n")
}
