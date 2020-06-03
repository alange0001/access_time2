package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type ratioArray []float64
type uintArray []uint

type optionsType struct {
	Directory            string
	FileSize             uint
	FilesystemPercent    uint
	NumberOfFiles        uint
	BlockSize            uintArray
	WriteRatio           ratioArray
	WriteRatioThread0    ratioArray
	RandomRatio          ratioArray
	Time                 uint
	Runs                 uint
	ExperimentMode       string
	ExperimentModeCreate bool
	ExperimentModeRun    bool
	ExperimentModeRemove bool
}

func (self *uintArray) String() string {
	return fmt.Sprintf("%v", *self)
}

func (self *uintArray) Set(value string) error {
	if len(value) > 0 {
		for _, item := range strings.Split(value, ",") {
			val, err := strconv.ParseUint(item, 10, 32)
			if err != nil {
				return err
			}
			*self = append(*self, uint(val))
		}
	}
	return nil
}

func (self *ratioArray) String() string {
	return fmt.Sprintf("%v", *self)
}

func (self *ratioArray) Set(value string) error {
	if len(value) > 0 {
		for _, item := range strings.Split(value, ",") {
			val, err := strconv.ParseFloat(item, 64)
			if err != nil {
				return err
			}
			if val < 0 || val > 1 {
				return errors.New("ratio must be between 0 and 1")
			}
			*self = append(*self, val)
		}
	}
	return nil
}

func parseArgs() (*optionsType, *syscall.Statfs_t) {
	var options optionsType
	flag.StringVar(&options.Directory, "directory", "", "working directory")
	flag.UintVar(&options.FileSize, "file-size", 100, "file size (MB)")
	flag.UintVar(&options.FilesystemPercent, "filesystem-percent", 0, "percent of the filesystem used in the experiment (override file-size)")
	flag.UintVar(&options.NumberOfFiles, "number-of-files", 1, "number of files to read/write")
	flag.Var(&options.BlockSize, "block-size", "block size (KB)")
	flag.Var(&options.WriteRatio, "write-ratio", "write ratio (writes/reads)")
	flag.Var(&options.WriteRatioThread0, "write-ratio-thread0", "write ratio of thread0 (writes/reads)")
	flag.Var(&options.RandomRatio, "random-ratio", "ramdom ratio (random/sequential)")
	flag.UintVar(&options.Time, "time", 7, "experiment time per run")
	flag.UintVar(&options.Runs, "runs", 1, "number of runs per write-ratio")
	flag.StringVar(&options.ExperimentMode, "experiment-mode", "create-and-run", "experiment mode: create-and-run (default), create, run, remove")
	flag.Parse()

	j, _ := json.Marshal(options)
	log.Printf("Options Received: %v", string(j))

	aux, err := os.Stat(options.Directory)
	if err != nil || !aux.IsDir() {
		log.Fatal("invalid directory")
	}
	if options.FileSize < 10 {
		log.Fatal("--file-size must be >= 10")
	}
	if options.FilesystemPercent > 100 {
		log.Fatal("--filesystem-percent must be <= 100")
	}
	if len(options.BlockSize) < 1 {
		options.BlockSize = []uint{1024}
	}
	if len(options.WriteRatio) < 1 {
		options.WriteRatio = []float64{0.0, 0.2, 0.4, 0.5, 0.6, 0.8, 1.0}
	}
	if len(options.RandomRatio) < 1 {
		options.RandomRatio = []float64{0.0, 0.2, 0.4, 0.5, 0.6, 0.8, 1.0}
	}
	if options.NumberOfFiles < 1 {
		log.Fatal("--number-of-files must be > 0")
	}
	if options.Time < 1 {
		log.Fatal("--time must be >= 1")
	}
	if options.Runs < 1 {
		log.Fatal("runs must be >= 1")
		if len(options.WriteRatio) > 1 {
			log.Fatal("--write-ratio must be defined with the option --runs")
		}
	}
	switch options.ExperimentMode {
	case "create-and-run":
		options.ExperimentModeCreate = true
		options.ExperimentModeRun = true
		options.ExperimentModeRemove = true
	case "create":
		options.ExperimentModeCreate = true
	case "run":
		options.ExperimentModeRun = true
	case "remove":
		options.ExperimentModeRemove = true
	default:
		log.Fatal("invalid --experiment-mode")
	}

	statfs := getFilesystemStats(options.Directory)
	if options.FilesystemPercent > 0 {
		percentBlocks := (uint(statfs.Blocks) * uint(statfs.Bsize)) / uint(1024*1024) // in MiB
		percentBlocks = (percentBlocks * options.FilesystemPercent) / uint(100)       // percentage
		options.FileSize = percentBlocks / uint(options.NumberOfFiles)                // per file/thread
		log.Printf("filesystem-percent defined to %v%%. Overriding file-size to %v", options.FilesystemPercent, options.FileSize)
	}
	for _, i := range options.BlockSize {
		if i < 4 || i > (options.FileSize*1024) {
			log.Fatal("invalid --block-size")
		}
		if ((i * 1024) % uint(statfs.Bsize)) != 0 {
			log.Fatal(fmt.Sprintf("--block-size must be multiple of %v KiB", statfs.Bsize/1024))
		}
	}

	j, _ = json.Marshal(options)
	log.Printf("Options Processed: %v", string(j))

	return &options, statfs
}

func main() {
	var wg sync.WaitGroup
	options, statfs := parseArgs()
	threads := make([]*threadType, options.NumberOfFiles)

	////// Creating data for the Threads and creating files //////
	for i := uint(0); i < options.NumberOfFiles; i++ {
		t := newThread(i, fmt.Sprintf("%v/%v", options.Directory, i), options, statfs)
		threads[i] = t
		if options.ExperimentModeCreate {
			t.createFile()
		} else {
			t.openFile()
		}
	}

	if options.ExperimentModeRun {
		////// Experiment Loop //////
		timeStart := time.Now()
		var ratiosThread0 ratioArray
		for _, ratio := range options.WriteRatio {
			if len(options.WriteRatioThread0) == 0 {
				ratiosThread0 = ratioArray{ratio}
			} else {
				ratiosThread0 = options.WriteRatioThread0
			}
			for _, ratioT0 := range ratiosThread0 {
				for _, randomRatio := range options.RandomRatio {
					for _, blockSize := range options.BlockSize {
						for runs := uint(0); runs < options.Runs; runs++ {
							for i, t := range threads {
								wg.Add(1)
								if i == 0 {
									go t.worker(blockSize, ratioT0, randomRatio, &wg)
								} else {
									go t.worker(blockSize, ratio, randomRatio, &wg)
								}
							}
							wg.Wait()
							// Print Results: //
							fmt.Printf("%.2f, %v, %.1f, %.1f, %.1f",
								time.Since(timeStart).Seconds(), blockSize, randomRatio, ratioT0, ratio)
							var sum uint64
							for _, t := range threads {
								sum += t.throughput
							}
							fmt.Printf(", %v", sum)
							for _, t := range threads {
								fmt.Printf(", %v", t.throughput)
							}
							fmt.Printf("\n")
						}
					}
				}
			}
		}
	}

	////// Removing Files //////
	if options.ExperimentModeRemove {
		for _, t := range threads {
			t.removeFile()
		}
	}
}

func getFilesystemStats(path string) *syscall.Statfs_t {
	var stats syscall.Statfs_t
	if err := syscall.Statfs(path, &stats); err != nil {
		log.Fatal(fmt.Sprintf("Impossible to read filesystem data from path %v", path))
	}
	log.Printf(fmt.Sprintf("Filesystem data: type=%v, block_size=%v, blocks=%v, blocks_free=%v, blocks_available=%v",
		stats.Type, stats.Bsize, stats.Blocks, stats.Bfree, stats.Bavail))

	return &stats
}

type threadType struct {
	id         uint
	options    *optionsType
	statfs     *syscall.Statfs_t
	filename   string
	fd         int
	throughput uint64 // MB/sec
}

func newThread(id uint, filename string, options *optionsType, statfs *syscall.Statfs_t) *threadType {
	var ret threadType
	ret.id = id
	ret.filename = filename
	ret.options = options
	ret.statfs = statfs
	return &ret
}

func (thread *threadType) createFile() {
	log.Printf("thread %v: Creating file %v", thread.id, thread.filename)
	fd, err := syscall.Open(thread.filename, syscall.O_CREAT|syscall.O_RDWR|syscall.O_DIRECT, 0600)
	if err != nil {
		log.Fatal(fmt.Sprintf("thread %v: Error creating file %v: %v", thread.id, thread.filename, err))
	}
	buffer := make([]byte, 2*1024*1024)
	rand.Read(buffer)
	for i := uint(0); i < thread.options.FileSize; i += 2 {
		if _, err := syscall.Write(fd, buffer); err != nil {
			log.Fatal(fmt.Sprintf("thread %v: Error writing file %v: %v", thread.id, thread.filename, err))
		}
	}
	if err := syscall.Fsync(fd); err != nil {
		log.Fatal(fmt.Sprintf("thread %v: Fsync error: %v", thread.id, err))
	}
	thread.fd = fd
}

func (thread *threadType) openFile() {
	var stats syscall.Stat_t
	log.Printf("thread %v: Creating file %v", thread.id, thread.filename)
	fd, err := syscall.Open(thread.filename, syscall.O_RDWR|syscall.O_DSYNC|syscall.O_DIRECT, 0600)
	if err != nil {
		log.Fatal(fmt.Sprintf("thread %v: Error opening file %v: %v", thread.id, thread.filename, err))
	}
	if err := syscall.Fstat(fd, &stats); err != nil {
		log.Fatal(fmt.Sprintf("thread %v: Fstat error : %v", thread.id, err))
	}
	if uint64(stats.Size) < uint64(thread.options.FileSize) {
		log.Fatal(fmt.Sprintf("thread %v: Invalid file size", thread.id))
	}

	thread.fd = fd
}

func (thread *threadType) removeFile() {
	log.Printf("thread %v: closing file %v", thread.id, thread.filename)
	if err := syscall.Close(thread.fd); err != nil {
		log.Fatal(fmt.Sprintf("thread %v: Error closing file %v: %v", thread.id, thread.filename, err))
	}
	log.Printf("thread %v: deleting file %v", thread.id, thread.filename)
	if err := syscall.Unlink(thread.filename); err != nil {
		log.Fatal(fmt.Sprintf("thread %v: Error removing file %v: %v", thread.id, thread.filename, err))
	}
}

func (thread *threadType) worker(blockSize uint, writeRatio float64, randomRatio float64, wg *sync.WaitGroup) {
	defer wg.Done()

	randomRatioInt := int32(randomRatio * 100)
	fileBlocks := int64((thread.options.FileSize * 1024) / blockSize)
	buffer := make([]byte, blockSize*1024)
	rand.Read(buffer)

	log.Printf(fmt.Sprintf("thread %v: main loop, blockSize=%v, writeRatio=%.1f, randomRatio=%.1f", thread.id, blockSize, writeRatio, randomRatio))
	var count uint64
	var r int64
	timeStart := time.Now()
	for uint(time.Since(timeStart).Seconds()) < thread.options.Time {
		for i := 0; i < 100; i++ {
			if randomRatioInt >= rand.Int31n(100) {
				r = rand.Int63n(fileBlocks)
				if _, err := syscall.Seek(thread.fd, r*1024, 0); err != nil {
					log.Fatal(fmt.Sprintf("thread %v seek error: %v", thread.id, err))
				}
			} else {
				r = (r + 1) % fileBlocks
				if r == 0 {
					if _, err := syscall.Seek(thread.fd, r*1024, 0); err != nil {
						log.Fatal(fmt.Sprintf("thread %v seek error: %v", thread.id, err))
					}
				}
			}
			if rand.Float64() < writeRatio {
				if _, err := syscall.Write(thread.fd, buffer); err != nil {
					log.Fatal(fmt.Sprintf("thread %v write error: %v", thread.id, err))
				}
			} else {
				if _, err := syscall.Read(thread.fd, buffer); err != nil {
					log.Fatal(fmt.Sprintf("thread %v read error: %v", thread.id, err))
				}
			}
			count++
		}
	}

	thread.throughput = uint64((float64(count*uint64(blockSize)) / float64(1024)) / time.Since(timeStart).Seconds())
	log.Printf("thread %v: count=%v, time=%.1f, throughput=%v", thread.id, count, time.Since(timeStart).Seconds(), thread.throughput)
}
