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
import numpy

class Options:
	format = 'png'

class DBClass:
	conn = sqlite3.connect(':memory:')
	file_id = 0

	def __init__(self):
		cur = self.conn.cursor()
		cur.execute('''CREATE TABLE files (
			  file_id INT PRIMARY KEY, FileName TEXT,
			  NumberOfFiles INT, FilesystemPercent INT, FileSize INT,
			  Runs INT, FixedWriteRatioThread0 BOOL)''')
		cur.execute('''CREATE TABLE data (
			file_id INT, data_id INT, Time NUMERIC(10,1),
			NumberOfFiles INT, FilesystemPercent INT, FileSize INT,
			BlockSize INT, RandomRatio NUMERIC(2,1), FixedWriteRatioThread0 BOOL,
			WriteRatioThread0 NUMERIC(2,1),
			WriteRatio NUMERIC(2,1), Total INT, Thread0 INT,
			PRIMARY KEY(file_id, data_id))''')
		self.conn.commit()

	def getFileId(self):
		ret = self.file_id
		self.file_id += 1
		return ret

	def getCursor(self):
		return self.conn.cursor()

	def query(self, sql, printsql=False):
		if printsql:
			print('SQL: ' + sql)
		return self.conn.cursor().execute(sql)

	def commit(self):
		self.conn.commit()

DB = DBClass()

class Graphs:
	filenames = None
	files = None
	fileids = None
	conn = None

	def __init__(self, path=None):
		self.filenames = []
		self.files = []
		self.fileids = []
		if path is not None:
			print("changing workdir to {}".format(path))
			os.chdir(path)
		for i in os.listdir():
			if re.search(r'\.csv$', i) is not None:
				self.filenames.append(i)
				f = File(i)
				self.files.append(f)
				self.fileids.append(f.id)

	def printAll(self):
		self.printFiles()
		self.printTotals(True)

	def printFiles(self):
		for file in self.files:
			file.print(True)

	def printTotals(self, save=False):
		fsp, bs = None, None
		for row_graphs in DB.query('''
			SELECT DISTINCT files.FilesystemPercent, data.BlockSize, data.RandomRatio
			FROM files, data
			WHERE files.file_id = data.file_id
			AND files.FixedWriteRatioThread0 = false
			ORDER BY files.FilesystemPercent, data.BlockSize, data.RandomRatio
		'''):
			fsp, bs, rr = row_graphs

			if float(rr) not in set([0.0, 0.5, 1.0]): continue

			fig1, ax1 = plt.subplots()
			fig2, ax2 = plt.subplots()
			fig1.set_figheight(5)
			fig2.set_figheight(5)
			fig1.set_figwidth(7)
			fig2.set_figwidth(7)

			for row_nf in DB.query('''
				SELECT DISTINCT NumberOfFiles
				FROM data
				WHERE FilesystemPercent = {} AND BlockSize = {}
				AND RandomRatio = {}
				AND FixedWriteRatioThread0 = false
				ORDER BY NumberOfFiles
			'''.format(fsp, bs, rr)):
				nf = row_nf[0]

				q = DB.query(
						'''SELECT WriteRatio, AVG(Total), AVG(Thread0)
						FROM data
						WHERE FilesystemPercent = {} AND BlockSize = {}
						AND RandomRatio = {}
						AND NumberOfFiles = {}
						AND FixedWriteRatioThread0 = false
						GROUP BY WriteRatio
						ORDER BY WriteRatio'''.format(fsp, bs, rr, nf)
					)
				A = numpy.array(q.fetchall()).T

				ax1.plot(A[0], A[1], '-', label=str(nf))
				ax2.plot(A[0], A[2], '-', label=str(nf))

			ax1.grid()
			ax2.grid()
			ax1.set(title='total: fs={fsp}%, bs={bs}, rand={rr}%'.format(fsp=fsp,bs=bs,rr=int(rr*100)),
			   xlabel='writes/reads', ylabel='MiB/s')
			ax2.set(title='thread0: fs={fsp}%, bs={bs}, rand={rr}%'.format(fsp=fsp,bs=bs,rr=int(rr*100)),
			   xlabel='writes/reads', ylabel='MiB/s (thread0)')
			chartBox = ax1.get_position()
			ax1.set_position([chartBox.x0, chartBox.y0, chartBox.width*0.9, chartBox.height])
			chartBox = ax2.get_position()
			ax2.set_position([chartBox.x0, chartBox.y0, chartBox.width*0.9, chartBox.height])
			ax1.legend(loc='upper center', bbox_to_anchor=(1.1, 0.8), title='threads', ncol=1, frameon=True)
			ax2.legend(loc='upper center', bbox_to_anchor=(1.1, 0.8), title='threads', ncol=1, frameon=True)
			if save:
				fig1.savefig('aggregated-fsp{fsp}bs{bs}rr{rr}-totals.{format}'.format(fsp=fsp,bs=bs,rr=int(rr*100), format=Options.format))
				fig2.savefig('aggregated-fsp{fsp}bs{bs}rr{rr}-thread0.{format}'.format(fsp=fsp,bs=bs,rr=int(rr*100), format=Options.format))
			plt.show()

	def queryFiles(self, sql):
		ret = []
		for row_file in DB.query(sql):
			id = row_file[0]
			ret.append(self.files[self.fileids[id]])
		return ret

	def getFile(self, filename):
		return self.files[self.filenames.index(filename)]

def tryConvert(value, *types):
	for t in types:
		try:
			ret = t(value)
			return ret
		except:
			pass
	return value

class File:
	id = None
	metadata = None
	data_keys = ['Time', 'BlockSize', 'RandomRatio', 'WriteRatioThread0', 'WriteRatio', 'Total', 'Thread0']

	def __init__(self, filename):
		self.id = DB.getFileId()
		self.metadata = collections.OrderedDict()
		self.metadata['file_id'] = self.id
		self.metadata['FileName'] = filename

		logfile = filename.replace('.csv', '.log')
		with open(logfile,newline='') as file:
			s = ''.join(file.readlines())
			options_re = re.findall(r'Options Processed: (\{[^}]+\})', s)
			if len(options_re) > 0:
				options = json.loads(options_re[0])
				#print(options)
				for k, v in options.items():
					self.metadata[k] = v
				self.metadata['FixedWriteRatioThread0'] = self.metadata['WriteRatioThread0'] is not None

		#print('============================================')
		#for k, v in self.metadata.items():
		#	print("{}: {}".format(k,v))

		cur = DB.getCursor()
		cur.execute('''INSERT INTO files
			  VALUES ({}, '{FileName}', {NumberOfFiles}, {FilesystemPercent},
			  {FileSize}, {Runs}, {FixedWriteRatioThread0})'''.format(	self.id, **self.metadata))

		data_dict = {
			'file_id'          :self.id,
			'data_id'          :0,
			'NumberOfFiles'    :self.metadata['NumberOfFiles'],
			'FilesystemPercent':self.metadata['FilesystemPercent'],
			'FileSize'         :self.metadata['FileSize'],
			'FixedWriteRatioThread0':self.metadata['FixedWriteRatioThread0'],
			}
		with open(filename,newline='') as file:
			reader = csv.reader(file, delimiter=',')
			for row in reader:
				for i in range(0, len(self.data_keys)):
					data_dict[self.data_keys[i]] = tryConvert(row[i].strip(' '), int, float)

				cur.execute('''INSERT INTO data VALUES(
					{file_id}, {data_id}, {Time},
					{NumberOfFiles}, {FilesystemPercent}, {FileSize},
					{BlockSize}, {RandomRatio}, {FixedWriteRatioThread0},
					{WriteRatioThread0}, {WriteRatio}, {Total}, {Thread0})'''.format(
					**data_dict))
				data_dict['data_id'] = data_dict['data_id'] + 1

		DB.commit()

	def print(self, save=False):
		'''
		if self.metadata['WriteRatioThread0'] == 1:
			self.printPerWriteRatio(save)
		else:
			self.printTelemetry(save)'''

	def printPerWriteRatio(self, save=False, printsql=False):
		ci = 0
		colors = plt.get_cmap('tab10').colors
		fig, ax = plt.subplots()
		fig.set_figheight(5)
		ax.grid()

		for bs in self.metadata['BlockSize']:
			for rr in [0, 0.5, 1]:
				q = DB.query('''SELECT WriteRatio, AVG(Total), AVG(Thread0)
					FROM data
					WHERE file_id = {} AND BlockSize = {} AND RandomRatio = {}
						AND WriteRatio = WriteRatioThread0
					GROUP BY WriteRatio ORDER BY WriteRatio'''.format(self.id, bs, rr),
					printsql=printsql)
				A = numpy.array(q.fetchall()).T
				ax.plot(A[0], A[1], '-', color=colors[ci], lw=1, label='total: bs={}, rand {}%'.format(bs, int(rr*100)))
				ax.plot(A[0], A[2], '.-', color=colors[ci], lw=1, label='thread0: bs={}, rand {}%'.format(bs, int(rr*100)))
				ci += 1

		ax.set(title='fs%={FilesystemPercent}, threads={NumberOfFiles}'.format(
			**self.metadata
			), xlabel='writes/reads', ylabel='MiB/s')

		chartBox = ax.get_position()
		ax.set_position([chartBox.x0, chartBox.y0, chartBox.width*0.7, chartBox.height])
		ax.legend(loc='upper center', bbox_to_anchor=(1.45, 0.9), title='threads', ncol=1, frameon=True)
		#ax.legend(loc='best', ncol=1, frameon=True)

		if save:
			save_name = '{}.{}'.format(self.metadata['FileName'].replace('.csv', ''), Options.format)
			fig.savefig(save_name)
		plt.show()

	def printTelemetry(self, save=False):
		'''
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
		'''


g = Graphs('exp5')
f = g.getFile('perc10files10.csv')
f.printPerWriteRatio()
