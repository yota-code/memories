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

"""
l'arborescence c'est
data_dir/
	public/
	.private/
	upload/
	_collision/
	.memories/
	.trash/
"""


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
	pot: str  # bucket name
	rel: Path  # path relative to data_dir
	suf: str = None
	dat: str = None
	hsh: str = None
	mtime: float = None

	def name(self):
		if self.dat:
			return f"{self.rel.fname}.{self.hsh[:16]}{self.suf}"
		else:
			return f"{self.dat}.{self.hsh[:6]}{self.suf}"

	def full(self):
		return f"{self.hsh}{self.suf}"


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

	d_map = {
		"upload": "upload",
		"public": "public",
		"private": ".private",
		"collision": "_collision",
		"unshared": "_unshared",  # picture not especially private, but not interesting enough to be shared
		"trash": ".trash",
	}

	def __init__(self):

		for v in self.d_map.values():
			(data_dir / v).make_dirs()

		self.memories_dir = data_dir / ".memories"
		self.memories_dir.make_dirs()

		self.l_map = {k: self.setup_list(k) for k in ["private", "unshared", "trash"]}

		self.f_map = collections.defaultdict(list)

		for v in self.d_map.values():
			self.scan_dir(data_dir / v)

		self.trash_dir.delete_content()

		Path(data_dir / "f_map.json").save(self.f_map, verbose=True)

	def __getattr__(self, name):
		if name.endswith("_dir") and name[:-4] in self.d_map:
			return data_dir / self.d_map[name[:-4]]
		else:
			raise AttributeError(
				f"{name} is not a valid attribute of class {self.__class__.__name__}"
			)

	def setup_list(self, name, prefix=""):
		"""un fichier .memories/_<name> contient la liste des hash
		tout fichier déjà présent dans le dossier correspondant est inscrit dans la liste
		tout fichier scanné matchant est déplacé dans le dossier correspondant
		"""
		list_pth = self.memories_dir / f"{name}.lst"

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

		# on récupère les hash des fichiers présents dans les sous dossiers pour compléter les listes
		list_dir.make_dirs()
		for suf in self.suffix_set:
			for pth in list_dir.glob("*" + suf):
				list_set.add(self.get_checksum(pth))

		list_pth.write_text("\n".join(sorted(list_set)))

		return list_pth, list_set

	@staticmethod
	def get_checksum(pth):
		with pth.open("rb") as fid:
			xx3 = xxhash.xxh3_128(fid.read())
		hsh = xx3.digest()
		b32 = base64.b32encode(hsh).decode("ascii")[:24]
		return b32 + f"_{pth.stat().st_size & 0xFF_FFFF:06X}"

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

	@staticmethod
	def get_file_info(pth: Path):

		hsh = Memories.get_checksum(pth)
		dat = Memories.get_datetime(pth)

		rel = pth.relative_to(data_dir).parts
		pot = rel[0] if rel else ""
		rel = Path(*rel[1:])

		suf = pth.suffix.lower()
		suf = Memories.replace_map.get(suf, suf)

		mtime = pth.stat().st_mtime

		return RenameFile(pot, rel, suf, dat, hsh, mtime)

	def scan_dir(self, pth=None):
		if pth is None:
			pth = data_dir

		t_start = time.perf_counter_ns()

		p_gen = pth.iter_on_suffix(
			*self.suffix_set,
		)

		i = 0
		with ThreadPoolExecutor() as pool:
			for obj in pool.map(Memories.get_file_info, p_gen):
				self.f_map[obj.hsh].append(obj)
				i += 1

		t_stop = time.perf_counter_ns()

		print(
			f"scanned {i} files in \x1b[33m{pth.relative_to(data_dir)}\x1b[0m, took {int((t_stop - t_start) / 1e6)}ms"
		)

	def move(self, src: Path, dst: Path, bucket: str, dry_run=True):
		"""
		les chemins doivent être relatifs à data_dir
		"""
		assert bucket in self.d_map, f"{bucket} is not a valid bucket"

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

		for hsh, o_lst in self.f_map.items():
			is_special = False
			for l, (l_pth, l_set) in self.l_map.items():
				# on teste voir si le hash est dans une des listes spéciales: private, unshared ou trash
				if hsh in l_set:
					is_special = True
					for i, obj in enumerate(o_lst):
						# si c'est le cas, on bouge chaque instance dans le dossier correspondant
						logger.info(f"{obj.hsh}\t{obj.rel} \x1b[31m--X\x1b[0m {l}")
						if i == 1:
							self.move(obj.rel, obj.name, l, dry_run)
						else:
							self.move(obj.rel, obj.full, "collision", dry_run)
					# seulement la première liste qui matche est considérée, on ne teste pas les autres listes
					break
			if is_special:
				continue

			for i, obj in enumerate(o_lst):
				logger.info(f"{obj.hsh}\t{sub}\t{dat}\t{src} \x1b[32m-->\x1b[0m {dst}")

			for i, obj in enumerate(self.f_map[key]):
				for l, (l_pth, l_set) in self.l_map.items():
					if obj.hsh in l_set:
						logger.info(f"{obj.hsh}\t{obj.src} \x1b[31m--X\x1b[0m {l}")
						self.apply(obj.src, l_pth)
				if obj.hsh in self.l_map[private][1]:
					logger.info(f"{obj.hsh}\t{obj.src} \x1b[31m--.\x1b[0m")
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
