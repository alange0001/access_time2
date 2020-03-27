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
	flag.UintVar(&options.numberOfFiles, "number-of-files", 1, "number of files to read/write")
	flag.UintVar(&options.blockSize, "block-size", 4, "block size (KB)")
	flag.Float64Var(&options.writeRatio, "write-ratio", -1, "write ratio (writes/reads)")
	flag.Float64Var(&options.writeRatioThread0, "write-ratio-thread0", -1, "write ratio of thread0 (writes/reads)")
	flag.UintVar(&options.time, "time", 7, "time")
	flag.Parse()

	log.Println("Options:", options)

	aux, err := os.Stat(options.directory)
	if err != nil || !aux.IsDir() {
		log.Fatal("invalid directory")
	}
	if options.fileSize < 10 {
		log.Fatal("file-size must be >= 10")
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
	ok := make(chan int)
	threads := make([]*Thread, options.numberOfFiles)

	for i := uint(0); i < options.numberOfFiles; i++ {
		t := NewThread(i, fmt.Sprintf("%v/%v", options.directory, i), options)
		threads[i] = t
		go t.createFile(ok)
	}
	for range threads {
		<-ok
	}

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
		fmt.Printf("%.2f, %v\n", ratio, threads[0].throughput)
	}

	for _, t := range threads {
		t.removeFile()
	}
}

type Thread struct {
	id         uint
	options    *Options
	filename   string
	fd         int
	throughput uint64 // MB/sec
}

func NewThread(id uint, filename string, options *Options) *Thread {
	var ret Thread
	ret.id = id
	ret.filename = filename
	ret.options = options
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
