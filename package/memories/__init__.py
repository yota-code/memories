#!/usr/bin/env python3

"""
sous manjaro, nécessite le paquet : exiftool
"""

import base64
import collections
import dataclasses
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

data_dir = (Path(os.environ["MEMORIES_root_DIR"]) / "data").resolve(strict=True)

assert data_dir.is_dir(), f"{data_dir} is not a directory"


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


@dataclasses.dataclass(slots=True)
class RenameFile:
	rel: Path  # path relative to data_dir
	suf: str = None
	dat: str = None
	hsh: str = None
	mtime: float = None


class RenameStatus(enum.Enum):
	DONE = 0
	RENAME = 1
	DUPLICATE = 2
	TRASH = -1
	PRIVATE = -2


hash16_rec = re.compile(r"^[A-Z2-7]{16}$")
processed_rec = re.compile(r"^[0-9]{8}_[0-9]{6}\.[A-Z2-7]{6}$")


class Memories:
	# and videos

	suffix_set = frozenset({".jpg", ".jpeg", ".png", ".mp4", ".mov"})
	replace_map = {".jpeg": ".jpg"}

	def __init__(self):

		if not (data_dir / ".memories").is_dir():
			(data_dir / ".memories").make_dirs()

		self.trash_pth, self.trash_set = self.setup_list("trash")
		self.private_pth, self.private_set = self.setup_list("private")

		(data_dir / "_collision").make_dirs()

		self.f_map = collections.defaultdict(list)

		self.scan_dir()
		self.scan_dir(data_dir / ".private")

	def setup_list(self, name):
		list_pth = data_dir / ".memories" / f"_{name}"
		if not list_pth.is_file():
			list_pth.write_text("")
			list_set = set()
		else:
			list_set = {
				line.strip()
				for line in list_pth.read_text().splitlines()
				if line.strip()
			}

		list_dir = data_dir / f".{name}"

		# on récupère les fichiers présents dans les sous dossiers pour compléter les listes
		list_dir.make_dirs()
		for suf in self.suffix_set:
			for pth in list_dir.glob("*" + suf):
				list_set.add(self.get_checksum(pth))

		list_pth.write_text("\n".join(sorted(list_set)))

		return list_pth, list_set

	def write_list(self, name, item):
		list_pth = data_dir / ".memories" / f"_{name}"
		with list_pth.open("at") as fid:
			fid.write(item + "\n")

	@staticmethod
	def get_checksum(pth):
		with pth.open("rb") as fid:
			xx3 = xxhash.xxh3_128(fid.read())
		hsh = xx3.digest()
		b32 = base64.b32encode(hsh).decode("ascii")[:24]
		return b32 + f"_{pth.stat().st_size & 0xFFFF_FFFF:08X}"

	@staticmethod
	def get_datetime(pth):
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

	def is_already_processed(self, pth):
		return hash16_rec.match(pth.suffixes[-2]) is not None or processed_rec.match(
			"".join(pth.suffixes[:2])
		)

	@staticmethod
	def get_file(pth: Path):

		hsh = Memories.get_checksum(pth)
		dat = Memories.get_datetime(pth)

		rel = pth.relative_to(data_dir)

		suf = pth.suffix.lower()
		suf = Memories.replace_map.get(suf, suf)

		mtime = pth.stat().st_mtime

		return RenameFile(rel, suf, dat, hsh, mtime)

		# if hsh in self.trash_set:
		# 	return (hsh, ".trash", dat, src, None)

		# if dat is None:
		# 	dst_pth = src_pth.parent / f"{src.fname}.{hsh[:16]}{suf}"
		# else:
		# 	dst_pth = src_pth.parent / f"{dat}.{hsh[:6]}{suf}"

		# dst = dst_pth.relative_to(self.cwd)

		# if hsh in self.private_set:
		# 	return (hsh, ".private", dat, src, dst)
		# else:
		# 	return (hsh, None, dat, src, dst)

	def scan_dir(self, pth=None):
		if pth is None:
			pth = data_dir

		t_start = time.perf_counter_ns()

		p_gen = pth.iter_on_suffix(*self.suffix_set)

		i = 0
		with ThreadPoolExecutor() as pool:
			for obj in pool.map(Memories.get_file, p_gen):
				self.f_map[obj.hsh].append(obj)
				i += 1

		t_stop = time.perf_counter_ns()

		print(f"scanned {i} files in {int((t_stop - t_start) / 1e6)}ms")

	def rename(self, src: Path, dst: Path, dry_run=True):
		"""
		les chemins doivent être relatifs à data_dir
		"""
		print(f"{src} -> {dst}")
		if not dry_run:
			src = (data_dir / src).resolve(strict=True)
			dst = (data_dir / dst).resolve()

			if dst.is_file():
				src_bin = src.read_bytes()
				dst_bin = dst.read_bytes()
				if src_bin != dst_bin:
					pass
			(data_dir / src).rename(data_dir / dst)

	def process(self, dry_run=True):
		if dry_run:
			logger = setup_logger("rename")
		else:
			logger = setup_logger("rename", self.log_pth, to_stdout=False)

		for key in self.f_map:
			for i, elem in enumerate(self.f_map[key]):
				sub, dat, src, dst = elem

				if sub is not None:
					logger.info(f"{key}\t{sub}\t{dat}\t{src} \x1b[31m--X\x1b[0m {sub}")
				else:
					if i == 0:
						logger.info(
							f"{key}\t{sub}\t{dat}\t{src} \x1b[32m-->\x1b[0m {dst}"
						)
					else:
						sub = ".duplicate"
						logger.info(
							f"{key}\t{sub}\t{dat}\t{src} \x1b[33m<->\x1b[0m {self.f_map[key][0][2]}"
						)
				if not dry_run:
					if sub is None:
						print(f"{src} -> {dst}")
						(self.cwd / src).rename(self.cwd / dst)
					else:
						print(f"{src} -> {sub}/{dst.fname}.{key}{dst.suffix}")
						(self.cwd / src).rename(
							data_dir / sub / f"{dst.fname}.{key}{dst.suffix}"
						)
