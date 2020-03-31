package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"syscall"
	"time"
)

type Options struct {
	Directory         string
	FileSize          uint
	FilesystemPercent uint
	NumberOfFiles     uint
	BlockSize         uint
	WriteRatio        float64
	WriteRatioThread0 float64
	Time              uint
	Runs              uint
}

func parseArgs() (*Options, *syscall.Statfs_t) {
	var options Options
	flag.StringVar(&options.Directory, "directory", "", "working directory")
	flag.UintVar(&options.FileSize, "file-size", 100, "file size (MB)")
	flag.UintVar(&options.FilesystemPercent, "filesystem-percent", 0, "percent of the filesystem used in the experiment (override file-size)")
	flag.UintVar(&options.NumberOfFiles, "number-of-files", 1, "number of files to read/write")
	flag.UintVar(&options.BlockSize, "block-size", 4, "block size (KB)")
	flag.Float64Var(&options.WriteRatio, "write-ratio", -1, "write ratio (writes/reads)")
	flag.Float64Var(&options.WriteRatioThread0, "write-ratio-thread0", -1, "write ratio of thread0 (writes/reads)")
	flag.UintVar(&options.Time, "time", 7, "experiment time per run")
	flag.UintVar(&options.Runs, "runs", 1, "number of runs per write-ratio")
	flag.Parse()

	j, _ := json.Marshal(options)
	log.Printf("Options Received: %v", string(j))

	aux, err := os.Stat(options.Directory)
	if err != nil || !aux.IsDir() {
		log.Fatal("invalid directory")
	}
	if options.FileSize < 10 {
		log.Fatal("file-size must be >= 10")
	}
	if options.FilesystemPercent > 100 {
		log.Fatal("filesystem-percent must be <= 100")
	}
	if options.BlockSize < 4 || options.BlockSize > (options.FileSize*1024) {
		log.Fatal("invalid block-size")
	}
	if options.NumberOfFiles < 1 {
		log.Fatal("number-of-files must be > 0")
	}
	if options.WriteRatio > 1 {
		log.Fatal("write-ratio must be between 0 and 1")
	}
	if options.WriteRatioThread0 > 1 {
		log.Fatal("write-ratio-thread0 must be between 0 and 1")
	}
	if options.Time < 1 {
		log.Fatal("time must be >= 1")
	}
	if options.Runs < 1 {
		log.Fatal("runs must be >= 1")
		if options.WriteRatio < 0 {
			log.Fatal("--write-ratio must be defined with the parameter --runs")
		}
	}

	statfs := getFilesystemStats(options.Directory)
	if options.FilesystemPercent > 0 {
		percentBlocks := (uint(statfs.Bavail) * uint(statfs.Bsize)) / uint(1024*1024) // in MiB
		percentBlocks = (percentBlocks * options.FilesystemPercent) / uint(100)       // percentage
		options.FileSize = percentBlocks / uint(options.NumberOfFiles)                // per file/thread
		log.Printf("filesystem-percent defined to %v%%. Overriding file-size to %v", options.FilesystemPercent, options.FileSize)
	}
	if ((options.BlockSize * 1024) % uint(statfs.Bsize)) != 0 {
		log.Fatal(fmt.Sprintf("block-size must be multiple of %v KiB", statfs.Bsize/1024))
	}

	j, _ = json.Marshal(options)
	log.Printf("Options Processed: %v", string(j))

	return &options, statfs
}

func main() {
	options, statfs := parseArgs()
	ok := make(chan int)
	threads := make([]*Thread, options.NumberOfFiles)

	////// Creating data for the Threads and creating files //////
	for i := uint(0); i < options.NumberOfFiles; i++ {
		t := NewThread(i, fmt.Sprintf("%v/%v", options.Directory, i), options, statfs)
		threads[i] = t
		go t.createFile(ok)
	}
	for range threads {
		<-ok
	}

	////// Defining writeRatios //////
	var writeRatios []float64
	if options.WriteRatio < 0 {
		writeRatios = make([]float64, 11)
		for i := 0; i < 11; i++ {
			writeRatios[i] = float64(i) / float64(10)
		}
	} else {
		writeRatios = make([]float64, 1)
		writeRatios[0] = options.WriteRatio
	}

	////// Experiment Loop //////
	timeStart := time.Now()
	var r float64
	for _, ratio := range writeRatios {
		for runs := uint(0); runs < options.Runs; runs++ {
			for i, t := range threads {
				if i == 0 && options.WriteRatioThread0 >= 0 {
					r = options.WriteRatioThread0
				} else {
					r = ratio
				}
				go t.processFile(r, ok)
			}
			for range threads {
				<-ok
			}
			ratioThread0 := ratio
			if options.WriteRatioThread0 >= 0 {
				ratioThread0 = options.WriteRatioThread0
			}
			// Print Results: //
			fmt.Printf("%.2f, %.1f, %.1f", time.Since(timeStart).Seconds(), ratioThread0, ratio)
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

	////// Removing Files //////
	for _, t := range threads {
		t.removeFile()
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

type Thread struct {
	id         uint
	options    *Options
	statfs     *syscall.Statfs_t
	filename   string
	fd         int
	throughput uint64 // MB/sec
}

func NewThread(id uint, filename string, options *Options, statfs *syscall.Statfs_t) *Thread {
	var ret Thread
	ret.id = id
	ret.filename = filename
	ret.options = options
	ret.statfs = statfs
	return &ret
}

func (thread *Thread) createFile(ok chan int) {
	log.Printf("thread %v: Creating file %v", thread.id, thread.filename)
	fdLocal, err := syscall.Open(thread.filename, syscall.O_CREAT|syscall.O_RDWR|syscall.O_DIRECT, 0600)
	if err != nil {
		log.Fatal(fmt.Sprintf("thread %v: Error creating file %v: %v", thread.id, thread.filename, err))
	}
	buffer := make([]byte, 1024*1024)
	rand.Read(buffer)
	for i := uint(0); i < thread.options.FileSize; i++ {
		if _, err := syscall.Write(fdLocal, buffer); err != nil {
			log.Fatal(fmt.Sprintf("thread %v: Error writing file %v: %v", thread.id, thread.filename, err))
		}
	}
	if err := syscall.Fsync(fdLocal); err != nil {
		log.Fatal(fmt.Sprintf("thread %v: Fsync error: %v", thread.id, err))
	}
	thread.fd = fdLocal
	ok <- 1
}

func (thread *Thread) removeFile() {
	log.Printf("thread %v: closing file %v", thread.id, thread.filename)
	if err := syscall.Close(thread.fd); err != nil {
		log.Fatal(fmt.Sprintf("thread %v: Error closing file %v: %v", thread.id, thread.filename, err))
	}
	log.Printf("thread %v: deleting file %v", thread.id, thread.filename)
	if err := syscall.Unlink(thread.filename); err != nil {
		log.Fatal(fmt.Sprintf("thread %v: Error removing file %v: %v", thread.id, thread.filename, err))
	}
}

func (thread *Thread) processFile(writeRatio float64, ok chan int) {
	fileBlocks := int64((thread.options.FileSize * 1024) / thread.options.BlockSize)
	buffer := make([]byte, thread.options.BlockSize*1024)
	rand.Read(buffer)

	log.Printf(fmt.Sprintf("thread %v: main loop, writeRatio=%.1f", thread.id, writeRatio))
	var count uint64
	timeStart := time.Now()
	for uint(time.Since(timeStart).Seconds()) < thread.options.Time {
		for i := 0; i < 100; i++ {
			r := rand.Int63n(fileBlocks)
			syscall.Seek(thread.fd, r*1024, 0)
			if rand.Float64() < writeRatio {
				if _, err := syscall.Write(thread.fd, buffer); err != nil {
					log.Fatal(fmt.Sprintf("thread %v: unable to write %v: %v", thread.id, thread.filename, err))
				}
			} else {
				if _, err := syscall.Read(thread.fd, buffer); err != nil {
					log.Fatal(fmt.Sprintf("thread %v: unable to read %v: %v", thread.id, thread.filename, err))
				}
			}
			count++
		}
	}

	thread.throughput = uint64((float64(count*uint64(thread.options.BlockSize)) / float64(1024)) / time.Since(timeStart).Seconds())
	log.Printf("thread %v: count=%v, time=%.1f, throughput=%v", thread.id, count, time.Since(timeStart).Seconds(), thread.throughput)
	ok <- 1
}
