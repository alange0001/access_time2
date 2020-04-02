#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 28 09:54:54 2020

@author: Adriano Lange <alange0001@gmail.com>
"""

import csv
import collections
import re
import sqlite3
import os
import json
import matplotlib.pyplot as plt

class Options:
	format = 'pdf'

class Graphs:
	filenames = None
	files = None
	conn = None

	def __init__(self, path=None):
		self.filenames = []
		self.files = []
		self.conn = sqlite3.connect(':memory:')
		cur = self.conn.cursor()
		cur.execute('CREATE TABLE files ('+
			  'id INTEGER PRIMARY KEY, FileName TEXT, BlockSize INT, '+
			  'NumberOfFiles INT, FilesystemPercent INT, FileSize INT, '+
			  'Runs INT, WriteRatio NUMERIC(3,1), WriteRatioThread0 NUMERIC(3,1))')
		idx = 0
		if path is not None:
			print("changing workdir to {}".format(path))
			os.chdir(path)
		for i in os.listdir():
			if re.search(r'\.csv$', i) is not None:
				self.filenames.append(i)
				f = File(i)
				self.files.append(f)
				cur.execute("INSERT INTO files VALUES ({}, '{}', {BlockSize}, {NumberOfFiles}, {FilesystemPercent}, {FileSize}, {Runs}, {WriteRatio}, {WriteRatioThread0})".format(
					idx, i, **f.metadata))
				idx = idx +1
		self.conn.commit()

	def printAll(self):
		self.printFiles()
		self.printTotals(True)

	def printFiles(self):
		for file in self.files:
			file.print(True)

	def printTotals(self, save=False):
		for row_group in self.query(
			'SELECT DISTINCT BlockSize, FilesystemPercent, WriteRatioThread0 '+
			'FROM files'
		):
			fig1, ax1 = plt.subplots()
			fig2, ax2 = plt.subplots()
			fig1.set_figheight(5)
			fig2.set_figheight(5)
			fig1.set_figwidth(7)
			fig2.set_figwidth(7)
			for f in self.queryFiles(
					'''SELECT id FROM files
					WHERE BlockSize = {} AND FilesystemPercent = {}
					AND WriteRatioThread0 = {} AND Runs=1 AND WriteRatio=-1
					ORDER BY NumberOfFiles'''.format(*row_group)
				):
				ax1.plot(f.data_writeratio, f.data_total, '-', label=str(f.metadata['NumberOfFiles']))
				ax2.plot(f.data_writeratio, f.data_thread0, '-', label=str(f.metadata['NumberOfFiles']))
			ax1.grid()
			ax2.grid()
			xlabel = 'writes/reads{}'.format('' if row_group[2] == -1 else ' (other threads)')
			title_t0 = '' if row_group[2] == -1 else ', thread0(w/r)={}'.format(row_group[2])
			ax1.set(title='total: bs={}, fs%={}{}'.format(row_group[0], row_group[1], title_t0),
			   xlabel=xlabel, ylabel='MiB/s')
			title_t0 = '' if row_group[2] == -1 else '(w/r={})'.format(row_group[2])
			ax2.set(title='thread0{}: bs={}, fs%={}'.format(title_t0, row_group[0], row_group[1]),
			   xlabel=xlabel, ylabel='MiB/s (thread0)')
			chartBox = ax1.get_position()
			ax1.set_position([chartBox.x0, chartBox.y0, chartBox.width*0.9, chartBox.height])
			chartBox = ax2.get_position()
			ax2.set_position([chartBox.x0, chartBox.y0, chartBox.width*0.9, chartBox.height])
			ax1.legend(loc='upper center', bbox_to_anchor=(1.1, 0.8), title='threads', ncol=1, frameon=True)
			ax2.legend(loc='upper center', bbox_to_anchor=(1.1, 0.8), title='threads', ncol=1, frameon=True)
			if save:
				filename_t0 = '' if row_group[2] == -1 else 'tz{}'.format(row_group[2])
				fig1.savefig('aggregated-bs{}fsp{}{}-totals.{format}'.format(row_group[0],row_group[1],filename_t0, format=Options.format))
				fig2.savefig('aggregated-bs{}fsp{}{}-thread0.{format}'.format(row_group[0],row_group[1],filename_t0, format=Options.format))
			plt.show()

	def query(self, sql):
		return self.conn.cursor().execute(sql)

	def queryFiles(self, sql):
		ret = []
		for row_file in self.query(sql):
			ret.append(self.files[row_file[0]])
		return ret

def tryConvert(value, *types):
	for t in types:
		try:
			ret = t(value)
			return ret
		except:
			pass
	return value

class File:
	metadata = None
	data = None
	data_time = None
	data_writeratio = None
	data_thread0 = None
	data_total = None
	def __init__(self, filename):
		self.metadata = collections.OrderedDict()
		self.metadata['FileName'] = filename

		self.data = []
		self.data_time = []
		self.data_writeratio = []
		self.data_thread0 = []
		self.data_total = []
		with open(filename,newline='') as file:
			reader = csv.reader(file, delimiter=',')
			for row in reader:
				aux = []
				for col in row:
					aux.append( tryConvert(col.strip(' '), int, float) )
				self.data.append(aux)
				self.data_time.append(aux[0])
				self.data_writeratio.append(aux[2])
				self.data_total.append(aux[3])
				self.data_thread0.append(aux[4])

		logfile = filename.replace('.csv', '.log')
		with open(logfile,newline='') as file:
			s = ''.join(file.readlines())
			options_re = re.findall(r'Options Processed: (\{[^}]+\})', s)
			if len(options_re) > 0:
				options = json.loads(options_re[0])
				#print(options)
				for k, v in options.items():
					self.metadata[k] = v

		#print('============================================')
		#for k, v in self.metadata.items():
		#	print("{}: {}".format(k,v))

	def print(self, save=False):
		if self.metadata['Runs'] == 1:
			self.printPerWriteRatio(save)
		else:
			self.printTelemetry(save)

	def printPerWriteRatio(self, save=False):
		fig, ax = plt.subplots()
		fig.set_figheight(5)
		ax.grid()
		ax.plot(self.data_writeratio, self.data_total,   '-', lw=1, color='blue', label='total')
		ax.plot(self.data_writeratio, self.data_thread0, '-', lw=1, color='orange', label='thread0')
		xlabel = 'writes/reads{}'.format('' if self.metadata['WriteRatioThread0'] == -1 else ' (other threads)')
		title_t0 = '' if self.metadata['WriteRatioThread0'] == -1 else '(w/r={})'.format(self.metadata['WriteRatioThread0'])
		ax.set(title='thread0{title_t0}: bs={BlockSize}, fs%={FilesystemPercent}, threads={NumberOfFiles}'.format(
			title_t0=title_t0, **self.metadata
			), xlabel=xlabel, ylabel='MiB/s')
		ax.legend(loc='best', ncol=1, frameon=True)
		if save:
			fig.savefig(self.metadata['FileName'].replace('.csv', '.{}'.format(Options.format)))
		plt.show()

	def printTelemetry(self, save=False):
		fig, ax = plt.subplots()
		fig.set_figheight(5)
		ax.grid()
		ax.plot(self.data_time, self.data_total,   '-', lw=1, color='blue', label='total')
		ax.plot(self.data_time, self.data_thread0, '-', lw=1, color='orange', label='thread0')
		title_t0 = '' if self.metadata['WriteRatioThread0'] == -1 else ', thread0 w/r={}'.format(self.metadata['WriteRatioThread0'])
		ax.set(title='bs={BlockSize}, fs%={FilesystemPercent}, threads={NumberOfFiles}{title_t0}, writes/reads={WriteRatio}'.format(
			title_t0=title_t0, **self.metadata
			), xlabel='time(s)', ylabel='MiB/s')
		ax.legend(loc='best', ncol=1, frameon=True)
		if save:
			fig.savefig(self.metadata['FileName'].replace('.csv', '.{}'.format(Options.format)))
		plt.show()


g = Graphs()
