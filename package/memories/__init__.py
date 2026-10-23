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
	.memories/
	.trash/


on part de l'hypothèse que le hash complet est unique, et que les fichiers avec le même hash sont forcément identiques et réciproquement
"""

dir_map = {
	"upload": "upload",  # dossier dans lequel on met les fichiers à traiter
	"public": "public",
	"private": ".private",
	"collision": "_collision",  # dossier dans lequel on met les fichiers qui ont le même hash mais un contenu différent
	"unshared": "_unshared",  # picture not especially private, but not interesting enough to be shared
	"trash": ".trash",
	None: "",
}

anon_set = {"collision", "trash"}


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
	rel: str  # path relative to data_dir/pot
	stm: str  # juste le nom brut, sans le suffixe, sans le hash
	suf: str = None
	dat: str = None
	hsh: str = None

	def __hash__(self):
		return self.hsh

	def resolve(self):
		return data_dir / self.pot / self.rel / self.name

	@property
	def name(self):
		if self.pot in anon_set:
			return f"{self.hsh}{self.suf}"
		if self.dat:
			return f"{self.dat}.{self.hsh[:6]}{self.suf}"
		else:
			return f"{self.stm}.{self.hsh[:16]}{self.suf}"


class RenameFolder:
	def __init__(self):
		self.__cache__ = {}

		self.file_lst = self.scan()

		self.check_for_collisions()

	def check_for_collisions(self):
		"""le plus gros défi c'est 2 fichiers qui auraient le même .name mais pas le même hash..."""
		p_map = self.sorted_by_nam_hsh()
		for nam, q_map in p_map.items():
			if len(q_map) != 1:
				print("collision for:")
				for hsh, obj in q_map.items():
					print("hsh", obj.resolve())

	def scan(self):
		"""démarre un scan"""

		t_start = time.perf_counter_ns()

		pth_gen = data_dir.iter_on_suffix(*self.suffix_set)

		with ThreadPoolExecutor() as pool:
			f_lst = [obj for obj in pool.map(self.file_info, pth_gen)]

		t_stop = time.perf_counter_ns()

		print(
			f"scanned {len(f_lst)} files in \x1b[33m{dir_map[pot]}\x1b[0m, took {int((t_stop - t_start) / 1e6)}ms"
		)

		return f_lst

	@staticmethod
	def file_info(pth: Path):

		pth = pth.resolve(strict=True)

		hsh = Memories.get_checksum(pth)
		dat = Memories.get_datetime(pth)

		p_lst = pth.relative_to(data_dir).parts
		pot = p_lst[0].lstrip("._")
		rel = Path(*p_lst[1:-1])

		nam = pth.fname

		suf = pth.suffix.lower()
		suf = Memories.replace_map.get(suf, suf)

		return RenameFile(pot, rel, nam, suf, dat, hsh)

	def sorted_by_pot(self):
		if "sorted_by_pot" not in self.__cache__:
			p_map = collections.defaultdict(set)
			for obj in self.file_lst:
				p_map[obj.pot].add(obj)
			self.__cache__["sorted_by_pot"] = p_map
		return self.__cache__["sorted_by_pot"]

	def sorted_by_name_hsh(self):
		if "sorted_by_nam_hsh" not in self.__cache__:
			p_map = collections.defaultdict(dict)
			for obj in self.file_lst:
				p_map[obj.nam][obj.hsh] = obj
			self.__cache__["sorted_by_nam_hsh"] = p_map
		return self.__cache__["sorted_by_nam_hsh"]


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

		for v in dir_map.values():
			(data_dir / v).make_dirs()

		self.memories_dir = data_dir / ".memories"
		self.memories_dir.make_dirs()

		self.log_pth = self.memories_dir / "rename.log"

		self.folder = RenameFolder()

		self.l_map = {k: self.setup_list(k) for k in ["private", "unshared", "trash"]}

		self.trash_dir.delete_content()

		Path(data_dir / "p_map.json").save(self.p_map, verbose=True)

	def __getattr__(self, name):
		if name.endswith("_dir") and name[:-4] in dir_map:
			return data_dir / dir_map[name[:-4]]
		else:
			raise AttributeError(
				f"{name} is not a valid attribute of class {self.__class__.__name__}"
			)

	def setup_list(self, pot):
		"""un fichier .memories/_<name> contient la liste des hash
		tout fichier déjà présent dans le dossier correspondant est inscrit dans la liste
		tout fichier scanné matchant est déplacé dans le dossier correspondant
		"""
		list_pth = self.memories_dir / f"{pot}.lst"

		if not list_pth.is_file():
			list_pth.write_text("")
			list_set = set()
		else:
			list_set = {
				line.strip()
				for line in list_pth.read_text().splitlines()
				if line.strip()
			}

		# on récupère les hash des fichiers présents dans les sous dossiers pour compléter les listes
		for obj in self.scan(pot):
			list_set.add(obj.hsh)

		list_pth.write_text("\n".join(sorted(list_set)))

		print(list_pth.relative_to(data_dir), "contains", len(list_set), "hashes")

		return list_pth, list_set

	@staticmethod
	def get_checksum(pth):
		xx3 = xxhash.xxh3_128()
		with pth.open("rb") as fid:
			while chunk := fid.read(2**24):
				xx3.update(chunk)
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
			key, _, value = line.partition(":")
			if "Create Date" == key.strip():
				try:
					return datetime.datetime.strptime(
						value.strip(), "%Y:%m:%d %H:%M:%S"
					).strftime("%Y%m%d_%H%M%S")
				except ValueError:
					pass
		return None

	def scan(self, pot: str = None):
		"""démarre un scan"""
		print(f"scan({pot})")

		t_start = time.perf_counter_ns()

		pth_gen = (data_dir / dir_map[pot]).iter_on_suffix(*self.suffix_set)

		with ThreadPoolExecutor() as pool:
			f_lst = [obj for obj in pool.map(Memories.get_file_info, pth_gen)]

		t_stop = time.perf_counter_ns()

		print(
			f"scanned {len(f_lst)} files in \x1b[33m{dir_map[pot]}\x1b[0m, took {int((t_stop - t_start) / 1e6)}ms"
		)

		return f_lst

	def move(self, obj: RenameFile, pot: str, dry_run=True):
		"""
		on prend un obj et on le copie dans le pot
		"""

		assert pot in dir_map, f"{pot} is not a valid bucket"

		src = obj.src_absolute()

		dst = obj.replace(pot=pot)

		if not dry_run:
			try:
				tgt = obj.replace(obj, rel=dst.relative_to)

			except KeyError:
				pass

			self.b_map[pot][str(obj.dst_relative())] = self.b_map[obj.pot].pop(
				str(obj.rel)
			)

		self.logger.info(
			f"{obj.hsh}\t{src.relative_to(data_dir)} -> {dst.relative_to(data_dir)}"
		)

	def process(self, dry_run=True):
		"""seuls les fichiers présents dans le dossier upload sont traités"""
		if dry_run:
			self.logger = setup_logger("rename")
		else:
			self.logger = setup_logger("rename", self.log_pth, to_stdout=False)

		for pth in list(self.b_map["upload"]):
			obj = self.b_map["upload"][pth]

			for pot, (l_pth, l_set) in self.l_map.items():
				# on teste voir si le hash est dans une des listes spéciales: private, unshared ou trash
				if obj.hsh in l_set:
					self.move(obj, pot, dry_run)
					break
			else:
				self.move(obj, "public", dry_run)
