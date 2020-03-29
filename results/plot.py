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
import matplotlib.pyplot as plt

class Graphs:
	filenames = None
	files = None
	conn = None

	def __init__(self):
		self.filenames = []
		self.files = []
		self.conn = sqlite3.connect(':memory:')
		cur = self.conn.cursor()
		cur.execute('CREATE TABLE files (id INTEGER PRIMARY KEY, name TEXT, block_size INT, number_of_files INT, filesystem_percent INT, file_size INT, write_ratio_thread0 NUMERIC(3,1))')

		idx = 0
		for i in os.listdir():
			if re.search(r'\.csv$', i) is not None:
				self.filenames.append(i)
				f = File(i)
				self.files.append(f)
				cur.execute("INSERT INTO files VALUES ({}, '{}', {}, {}, {}, {}, {})".format(
					idx, i, f.metadata['block-size'], f.metadata['number-of-files'],
					f.metadata['filesystem-percent'], f.metadata['file-size'],
					f.metadata['write-ratio-thread0']))
				idx = idx +1
		self.conn.commit()

	def main(self):
		self.printFiles()
		self.printTotals()

	def printFiles(self):
		for file in self.files:
			fig, ax = plt.subplots()
			fig.set_figheight(5)
			ax.grid()
			ax.plot(file.x, file.y_thread0, '-', lw=1, color='blue', label='thread 0')
			ax.plot(file.x, file.y_total, '-', lw=1, color='orange', label='total')
			ax.set_xlabel('writes/reads')
			ax.set_ylabel('MiB/s')
			ax.legend(loc='best', ncol=2, frameon=True)
			fig.savefig(file.metadata['filename'].replace('.csv', '.pdf'))
			plt.show()

	def printTotals(self):
		for row_percent in self.conn.cursor().execute('SELECT DISTINCT filesystem_percent FROM files ORDER BY filesystem_percent'):
			#print(row_percent[0])
			fig1, ax1 = plt.subplots()
			fig2, ax2 = plt.subplots()
			fig1.set_figheight(5)
			fig2.set_figheight(5)
			for row_file in self.conn.cursor().execute('SELECT id, number_of_files FROM files WHERE filesystem_percent = {} AND write_ratio_thread0 = -1 ORDER BY number_of_files'.format(row_percent[0])):
				f = self.files[row_file[0]]
				ax1.plot(f.x, f.y_total, '-', label=str(row_file[1]))
				ax2.plot(f.x, f.y_thread0, '-', label=str(row_file[1]))
			ax1.grid()
			ax2.grid()
			ax1.set_xlabel('writes/reads')
			ax2.set_xlabel('writes/reads')
			ax1.set_ylabel('MiB/s')
			ax2.set_ylabel('MiB/s')
			ax1.legend(loc='best', ncol=2, frameon=True)
			ax2.legend(loc='best', ncol=2, frameon=True)
			fig1.savefig('perc{}totals.pdf'.format(row_percent[0]))
			fig2.savefig('perc{}thread0.pdf'.format(row_percent[0]))
			plt.show()

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
	x = None
	y_thread0 = None
	y_total = None
	def __init__(self, filename):
		self.metadata = collections.OrderedDict()
		self.metadata['filename'] = filename

		self.data = []
		self.x = []
		self.y_thread0 = []
		self.y_total = []
		with open(filename,newline='') as file:
			reader = csv.reader(file, delimiter=',')
			for row in reader:
				aux = []
				for col in row:
					aux.append( tryConvert(col.strip(' '), int, float) )
				self.data.append(aux)
				self.x.append(aux[0])
				self.y_thread0.append(aux[1])
				self.y_total.append(aux[2])

		logfile = filename.replace('.csv', '.log')
		with open(logfile,newline='') as file:
			s = ''.join(file.readlines())
			self.metadata['block-size'] = tryConvert(re.findall(r'block-size=([0-9]+)', s)[0], int, float)
			self.metadata['number-of-files'] = tryConvert(re.findall(r'number-of-files=([0-9]+)', s)[0], int, float)
			self.metadata['filesystem-percent'] = tryConvert(re.findall(r'filesystem-percent=([0-9]+)', s)[0], int, float)
			self.metadata['file-size'] = tryConvert(re.findall(r'file-size=([0-9]+)', s)[0], int, float)
			aux = re.findall(r'file-size to ([0-9]+)', s)
			if len(aux) > 0: self.metadata['file-size'] = tryConvert(aux[0], int, float)
			self.metadata['write-ratio-thread0'] = tryConvert(re.findall(r'write-ratio-thread0=([-.0-9]+)', s)[0], int, float)

		#print('============================================')
		#for k, v in self.metadata.items():
		#	print("{}: {}".format(k,v))

g = Graphs()
g.main()
