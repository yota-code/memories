#!/usr/bin/env python3

"""
sous manjaro, nécessite le paquet : exiftool
"""

import base64
import collections
import datetime
import enum
import functools
import logging
import mmap
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import xxhash
from cc_pathlib import Path


def setup_logger(name, log_file=None, to_stdout=True, level=logging.INFO):
	logger = logging.getLogger(name)
	logger.setLevel(level)

	if logger.hasHandlers():
		logger.handlers.clear()

	if to_stdout:
		logger.addHandler(logging.StreamHandler(sys.stdout))

	if log_file:
		logger.addHandler(logging.FileHandler(log_file))

	return logger


class RenameStatus(enum.Enum):
	ALREADY = 0
	RENAME = 1
	DUPLICATE = 2
	TRASH = -1


hash16_rec = re.compile(r"^[A-Z2-7]{16}$")
processed_rec = re.compile(r"^[0-9]{8}_[0-9]{6}\.[A-Z2-7]{6}$")


class Memories:
	# and videos

	suffix_set = frozenset({".jpg", ".jpeg", ".png", ".mp4", ".mov"})
	replace_map = {".jpeg": ".jpg"}

	def __init__(self, cwd=None):
		self.data_dir = (Path(os.environ["MEMORIES_root_DIR"]) / "data").resolve(
			strict=True
		)
		assert self.data_dir.is_dir()

		self.cwd = self.data_dir if cwd is None else Path(cwd).resolve(strict=True)
		self.cwd.relative_to(self.data_dir)

		if not (self.data_dir / ".memories").is_dir():
			(self.data_dir / ".memories").make_dirs()

		self.trash_pth = self.data_dir / ".memories" / "_trash"
		if not self.trash_pth.is_file():
			self.trash_pth.write_text("")
			self.trash_set = set()
		else:
			self.trash_set = {
				line.strip()
				for line in self.trash_pth.read_text().splitlines()
				if line.strip()
			}

		self.log_pth = self.data_dir / ".memories" / "rename.log"

		self.scan_dir()

	def get_checksum_old(self, pth):
		with (
			pth.open("rb") as fid,
			mmap.mmap(fid.fileno(), 0, prot=mmap.PROT_READ) as mmp,
		):
			xx3 = xxhash.xxh3_128()
			xx3.update(mmp)
			hsh = xx3.digest()
			b32 = base64.b32encode(hsh).decode("ascii")[:24]
		return b32, pth.stat().st_size

	def get_checksum(self, pth):
		with pth.open("rb") as fid:
			xx3 = xxhash.xxh3_128(fid.read())
			hsh = xx3.digest()
			b32 = base64.b32encode(hsh).decode("ascii")[:24]
		return b32 + f"_{pth.stat().st_size & 0xFFFF_FFFF:08X}"

	def is_already_processed(self, pth):
		return hash16_rec.match(pth.suffixes[-2]) is not None or processed_rec.match(
			"".join(pth.suffixes[:2])
		)

	def get_datetime(self, pth):
		"""utilise exiftool pour trouver un date dans les métadonnées"""
		""" TAG

		Extract information for the specified tag (eg. -CreateDate)."""
		ret = subprocess.run(
			["exiftool", str(pth)], stdout=subprocess.PIPE, check=False
		)
		for line in ret.stdout.decode("utf8", errors="replace").splitlines():
			key, sep, value = line.partition(":")
			if "Create Date" == key.strip():
				try:
					return datetime.datetime.strptime(
						value.strip(), "%Y:%m:%d %H:%M:%S"
					).strftime("%Y%m%d_%H%M%S")
				except ValueError:
					pass
		return None

	def scan_file(self, cwd, src_pth):

		hsh = self.get_checksum(src_pth)
		dat = self.get_datetime(src_pth)

		if hsh in self.trash_set:
			return (hsh, dat, src_pth.relative_to(cwd), None)

		suf = self.replace_map.get(src_pth.suffix.lower(), src_pth.suffix.lower())

		if dat is None:
			dst_pth = src_pth.parent / f"{src_pth.fname}.{hsh[:16]}{suf}"
		else:
			dst_pth = src_pth.parent / f"{dat}.{hsh[:6]}{suf}"

		return (hsh, dat, src_pth.relative_to(cwd), dst_pth.relative_to(cwd))

	def scan_dir(self):
		t_start = time.perf_counter_ns()
		p_gen = self.cwd.iter_on_suffix(*self.suffix_set)
		p_fct = functools.partial(Memories.scan_file, self, self.cwd)

		self.f_map = collections.defaultdict(list)

		i = 0
		with ThreadPoolExecutor() as pool:
			for hsh, dat, src, dst in pool.map(p_fct, p_gen):
				self.f_map[hsh].append((dat, src, dst))
				i += 1

		t_stop = time.perf_counter_ns()

		print(f"scan {i} files in {int((t_stop - t_start) / 1e6)}ms")

	def process(self, dry_run=True):
		logger = setup_logger("rename", log_file=None if dry_run else self.log_pth)
		for key in self.f_map:
			for i, elem in enumerate(self.f_map[key]):
				dat, src, dst = elem
				if dst is None:
					logger.info(f"{key}\t{dat}\t{src} --X")
					continue
				if i == 0:
					logger.info(f"{key}\t{dat}\t{src} --> {dst}")
				else:
					logger.info(f"{key}\t{dat}\t{src} <-> {self.f_map[key][0][1]}")
