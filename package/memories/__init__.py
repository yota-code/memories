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
# format_rec = re.compile(r"^[0-9]{8}_[0-9]{6}\.[A-Z2-7]{6}$")


class Memories:
	# and videos

	# hashed_res = re.compile(r"(([0-9]{8}_[0-9]{6}\.[A-Z2-7]{6})|(\.[A-Z2-7]{16}))$")

	suffix_set = frozenset({".jpg", ".jpeg", ".png", ".mp4", ".mov"})
	replace_map = {".jpeg": ".jpg"}

	def __init__(self):
		self.data_dir = (Path(os.environ["MEMORIES_root_DIR"]) / "data").resolve(
			strict=True
		)
		assert self.data_dir.is_dir()

		if not (self.data_dir / ".memories").is_dir():
			(self.data_dir / ".memories").make_dirs()

		self.trash_pth = self.data_dir / ".memories" / "trash"
		if not self.trash_pth.is_file():
			self.trash_pth.write_text("")
		else:
			self.trash_set = {
				line.strip()
				for line in self.trash_pth.read_text().splitlines()
				if line.strip()
			}

		self.rename_log = setup_logger(
			"rename", (self.data_dir / ".memories" / "rename.log").make_parents()
		)
		self.rename_log.info(f"tutu {hash(self)}")

	def get_checksum(self, pth):
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
		return b32, pth.stat().st_size

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
		# print(f"rename_file({cwd}, {src_pth})")

		# if self.hashed_res.search(src_pth.stem) is not None :
		# 	# on dirait bien que le boulot est déjà fait
		# 	return

		key = self.get_checksum(src_pth)
		hsh = key[0]
		dat = self.get_datetime(src_pth)

		if hsh in self.trash_set:
			return (key, dat, src_pth.relative_to(cwd), None)

		suf = self.replace_map.get(src_pth.suffix.lower(), src_pth.suffix.lower())

		if dat is None:
			# val = src_pth.suffixes[-2].strip(".")
			# if hash16_rec.match(val) is not None and hash16_rec.group(0) == val:
			# 	dst_pth = src_pth.parent / f"{src_pth.fname}.{hsh[:16]}{suf}"
			dst_pth = src_pth.parent / f"{src_pth.fname}.{hsh[:16]}{suf}"
		else:
			dst_pth = src_pth.parent / f"{dat}.{hsh[:6]}{suf}"

		return (key, dat, src_pth.relative_to(cwd), dst_pth.relative_to(cwd))

	def process_file(self, cwd, key, dry_run=True):
		dat, src_pth, dst = self.f_ma[key]

		if dst_pth is None:
			return RenameStatus.TRASH

		if src_pth == dst_pth:
			return RenameStatus.ALREADY

		if dst_pth.exists():
			if are_file_same_inodev(src_pth, dst_pth):
				return RenameStatus.ALREADY
			elif are_files_identical(src_pth, dst_pth):
				return RenameStatus.DUPLICATE
			else:
				return RenameStatus.RENAME

		return RenameStatus.RENAME

	def rename_dir(self, cwd=None, dry_run=True):
		cwd = (Path(cwd) if cwd is not None else self.data_dir).resolve()

		print(f">>> rename_dir({cwd})")

		p_gen = cwd.iter_on_suffix(*self.suffix_set)
		p_fct = functools.partial(Memories.scan_file, self, cwd)

		f_map = collections.defaultdict(list)

		with ThreadPoolExecutor() as pool:
			for key, dat, src, dst in pool.map(p_fct, p_gen):
				f_map[key].append((dat, src, dst))
				self.rename_log.info(
					f"{key}\t{cwd.relative_to(self.data_dir)} / {src} -> {dst}"
				)


if __name__ == "__main__":
	import sys

	u = Memories(Path(sys.argv[1]))
	u.rename(Path(sys.argv[2]))
