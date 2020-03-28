package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"syscall"
	"time"
)

type Options struct {
	directory         string
	fileSize          uint
	filesystemPercent uint
	numberOfFiles     uint
	blockSize         uint
	writeRatio        float64
	writeRatioThread0 float64
	time              uint
}

func parseArgs() *Options {
	var options Options
	flag.StringVar(&options.directory, "directory", "", "working directory")
	flag.UintVar(&options.fileSize, "file-size", 100, "file size (MB)")
	flag.UintVar(&options.filesystemPercent, "filesystem-percent", 0, "percent of the filesystem used in the experiment (override file-size)")
	flag.UintVar(&options.numberOfFiles, "number-of-files", 1, "number of files to read/write")
	flag.UintVar(&options.blockSize, "block-size", 4, "block size (KB)")
	flag.Float64Var(&options.writeRatio, "write-ratio", -1, "write ratio (writes/reads)")
	flag.Float64Var(&options.writeRatioThread0, "write-ratio-thread0", -1, "write ratio of thread0 (writes/reads)")
	flag.UintVar(&options.time, "time", 7, "time")
	flag.Parse()

	log.Println("Options:")
	log.Printf("   directory=%v, file-size=%v, filesystem-percent=%v, "+
		"number-of-files=%v, block-size=%v",
		options.directory, options.fileSize, options.filesystemPercent,
		options.numberOfFiles, options.blockSize)
	log.Printf("   write-ratio=%v, write-ratio-thread0=%v, time=%v",
		options.writeRatio, options.writeRatioThread0, options.time)

	aux, err := os.Stat(options.directory)
	if err != nil || !aux.IsDir() {
		log.Fatal("invalid directory")
	}
	if options.fileSize < 10 {
		log.Fatal("file-size must be >= 10")
	}
	if options.filesystemPercent > 100 {
		log.Fatal("filesystem-percent must be <= 100")
	}
	if options.blockSize < 4 || options.blockSize > (options.fileSize*1024) {
		log.Fatal("invalid block-size")
	}
	if options.numberOfFiles < 1 {
		log.Fatal("number-of-files must be > 0")
	}
	if options.writeRatio > 1 {
		log.Fatal("write-ratio must be between 0 and 1")
	}
	if options.writeRatioThread0 > 1 {
		log.Fatal("write-ratio-thread0 must be between 0 and 1")
	}
	if options.time < 5 {
		log.Fatal("time must be >= 5")
	}

	return &options
}

func main() {
	options := parseArgs()
	statfs := getFilesystemStats(options.directory)
	ok := make(chan int)
	threads := make([]*Thread, options.numberOfFiles)

	if options.filesystemPercent > 0 {
		percentBlocks := (uint(statfs.Bavail) * uint(statfs.Bsize)) / uint(1024*1024) // in MiB
		percentBlocks = (percentBlocks * options.filesystemPercent) / uint(100)       // percentage
		options.fileSize = percentBlocks / uint(options.numberOfFiles)                // per file/thread
		log.Printf("filesystem-percent defined to %v%%. Overriding file-size to %v", options.filesystemPercent, options.fileSize)
	}
	if ((options.blockSize * 1024) % uint(statfs.Bsize)) != 0 {
		log.Fatal(fmt.Sprintf("block-size must be multiple of %v KiB", statfs.Bsize/1024))
	}

	////// Creating data for the Threads and creating files //////
	for i := uint(0); i < options.numberOfFiles; i++ {
		t := NewThread(i, fmt.Sprintf("%v/%v", options.directory, i), options, statfs)
		threads[i] = t
		go t.createFile(ok)
	}
	for range threads {
		<-ok
	}

	////// Defining writeRatios //////
	var writeRatios []float64
	if options.writeRatio < 0 {
		writeRatios = make([]float64, 11)
		for i := 0; i < 11; i++ {
			writeRatios[i] = float64(i) / float64(10)
		}
	} else {
		writeRatios = make([]float64, 1)
		writeRatios[0] = options.writeRatio
	}

	////// Experiment Loop //////
	var r float64
	for _, ratio := range writeRatios {
		for i, t := range threads {
			if i == 0 && options.writeRatioThread0 >= 0 {
				r = options.writeRatioThread0
			} else {
				r = ratio
			}
			go t.processFile(r, ok)
		}
		for range threads {
			<-ok
		}
		var sum uint64
		for _, t := range threads {
			sum += t.throughput
		}
		fmt.Printf("%.2f, %v, %v\n", ratio, threads[0].throughput, sum)
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
	for i := uint(0); i < thread.options.fileSize; i++ {
		if _, err := syscall.Write(fdLocal, buffer); err != nil {
			log.Fatal(fmt.Sprintf("thread %v: Error writing file %v: %v", thread.id, thread.filename, err))
		}
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
	fileBlocks := int64((thread.options.fileSize * 1024) / thread.options.blockSize)
	buffer := make([]byte, thread.options.blockSize*1024)
	rand.Read(buffer)

	log.Printf(fmt.Sprintf("thread %v: main loop, writeRatio=%.2f", thread.id, writeRatio))
	var count uint64
	timeStart := time.Now()
	for uint(time.Since(timeStart).Seconds()) < thread.options.time {
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

	thread.throughput = uint64((float64(count*uint64(thread.options.blockSize)) / float64(1024)) / time.Since(timeStart).Seconds())
	log.Printf("thread %v: count=%v, time=%.1f, throughput=%v", thread.id, count, time.Since(timeStart).Seconds(), thread.throughput)
	ok <- 1
}
