#!/bin/bash

workdir='/media/auto/work/tmp'
outputdir='.'
[ ! -z "$1" ] && outputdir="$1"

function main() {
	for p in 30 80; do
		for ((i=1;i<7;i++)); do
			runExperiment "$i" "$p"
		done
	done
}

function runExperiment() {
	i="$1"
	p="$2"

	go run ../access_time2.go \
		--experiment-mode=create \
		--directory="$workdir" \
		--number-of-files=$i \
		--filesystem-percent=$p
	sleep 120
	go run ../access_time2.go \
		--experiment-mode=run \
		--directory="$workdir" \
		--number-of-files=$i \
		--filesystem-percent=$p \
		--block-size="4,8,128,256,512" \
		--random-ratio="0,0.5,1" \
		--time=3 \
		--runs=5 \
		>"$outputdir/perc${p}files${i}.csv" \
		2>"$outputdir/perc${p}files${i}.log"
	go run ../access_time2.go \
		--experiment-mode=remove \
		--directory="$workdir" \
		--number-of-files=$i \
		--filesystem-percent=$p
}

main
