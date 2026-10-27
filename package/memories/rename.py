#!/usr/bin/env python3

"""
sous manjaro, nécessite le paquet : exiftool
"""

import base64
import collections
import datetime
import json
import os
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import xxhash
from cc_pathlib import Path

data_dir = (Path(os.environ["MEMORIES_root_DIR"]) / "data").resolve(strict=True)
data_len = len(str(data_dir)) + 1

os.chdir(data_dir)

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
	"unshared": "_unshared",  # picture not especially private, but not interesting enough to be shared
	"trash": ".trash",
}

suffix_set = {".jpg", ".jpeg", ".png", ".mp4", ".mov"}
suffix_map = {".jpeg": ".jpg"}


"""
dans l'ordre des trucs à faire :

initialiser trash, private et unshare, récupérer le hash et compléter la liste

scanner public

supprimer les fichiers de la liste qui auraient pu se glisser dans public et les mettre dans trash, private ou unshare

normaliser les noms de fichiers dans public, private et unshare au besoin

   >>> le seul rangement qu'il nous faut c'est p_ma[pot][hsh] = { file, file, file, etc... } avec la possibilité de très rapidement fusionner les pot

checker les collisions dans upload, private, unshared, public mais PAS collision

transférer les fichiers d'upload dans leurs dossiers respectifs


vider la poubelle

"""


def get_exif_date(pth_set):
	with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as fid:
		fid.write("\n".join(pth_set))
		fid.flush()

		ret = data_dir.run("exiftool", "-j", "-CreateDate", "-@", fid.name)
		if ret.returncode == 0:
			return json.loads(ret.stdout)

	return []


class MemoriesRename:
	# and videos

	special_set = frozenset(["private", "unshared", "trash"])

	def __init__(self):

		self.pth_to_hsh = {}
		self.bkt_to_pth = collections.defaultdict(set)

		self.hsh_to_dat = {}

		self.pth_to_nam = {}

		self.bkt_to_pth = collections.defaultdict(set)

		for v in dir_map.values():
			(data_dir / v).make_dirs()

		self.memories_dir = data_dir / ".memories"
		self.memories_dir.make_dirs()

		self.log_pth = self.memories_dir / "rename.log"

	def process(self):

		for bkt in dir_map:
			p_map = self.scan_file(bkt)
			self.pth_to_hsh |= p_map
			self.bkt_to_pth[bkt] = set(p_map)

		(self.memories_dir / "pth_to_hsh.json").save(self.pth_to_hsh, verbose=True)
		(self.memories_dir / "bkt_to_pth.json").save(self.bkt_to_pth, verbose=True)

		self.fetch_date()

		self.bkt_to_hsh = {bkt: self.setup_special(bkt) for bkt in self.special_set}
		self.hsh_to_bkt = {
			hsh: bkt for bkt, hsh_lst in self.bkt_to_hsh.items() for hsh in hsh_lst
		}
		(self.memories_dir / "bkt_to_hsh.json").save(self.bkt_to_hsh, verbose=True)
		(self.memories_dir / "hsh_to_bkt.json").save(self.hsh_to_bkt, verbose=True)

		self.check_collision()

		self.rename_all()

	def iter_bkt_pth_hsh(self, *include, exclude=None):
		bkt_set = set(include if include else set(dir_map)) - set(exclude)

		for bkt in bkt_set:
			for pth in self.bkt_to_pth[bkt]:
				yield bkt, pth, self.pth_to_hsh[pth]

	def get_name(self, pth, is_anonymous=False):
		hsh = self.pth_to_hsh[pth]

		nam = pth.split("/")[-1].split(".")
		stm = nam[0]
		suf = suffix_map.get(nam[-1], nam[-1])

		if is_anonymous:
			return f"{hsh}.{suf}"

		dat = self.hsh_to_dat.get(hsh, None)

		if dat:
			return f"{dat}.{hsh[:6]}.{suf}"
		else:
			return f"{stm}.{hsh[:16]}.{suf}"

	def build_bkt_to_pth(self):
		rev_map = {pre: bkt for bkt, pre in dir_map.items() if bkt is not None}
		print(rev_map)
		for pth in self.pth_to_hsh:
			pre, _, post = pth.partition("/")
			if pre in rev_map:
				self.bkt_to_pth[rev_map[pre]].add(pth)
			else:
				self.bkt_to_pth[""].add(pth)

		(self.memories_dir / "bkt_to_pth.json").save(self.bkt_to_pth, verbose=True)

	def setup_special(self, bkt):
		"""un fichier .memories/_<name> contient la liste des hash
		tout fichier déjà présent dans le dossier correspondant est inscrit dans la liste
		tout fichier scanné matchant est déplacé dans le dossier correspondant
		"""
		list_pth = self.memories_dir / f"{bkt}.lst"

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
		list_set |= {self.pth_to_hsh[pth] for pth in self.bkt_to_pth[bkt]}

		list_pth.write_text("\n".join(sorted(list_set)))

		print(list_pth.relative_to(data_dir), "contains", len(list_set), "hashes")

		return list_set

	@staticmethod
	def get_hsh_siz(pth):
		xx3 = xxhash.xxh3_128()
		with pth.open("rb") as fid:
			while chunk := fid.read(2**24):
				xx3.update(chunk)
		hsh = xx3.digest()
		b32 = base64.b32encode(hsh).decode("ascii")[:24]
		return pth, b32, pth.stat().st_size
		# return b32 + f"_{pth.stat().st_size & 0xFF_FFFF:06X}"

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

	def scan_file(self, bkt):
		"""on itère sur tous les fichiers dont l'extension est dans suffix_set
		et on récupère le hash et la taille du fichier
		"""

		def checksum(dir_entry):
			xx3 = xxhash.xxh3_128()
			with open(dir_entry.path, "rb") as fid:
				while chunk := fid.read(2**16):
					xx3.update(chunk)
			hsh = xx3.digest()
			hsh = base64.b32encode(hsh).decode("ascii")[:24]
			siz = dir_entry.stat().st_size & 0xFF_FFFF
			hsh = f"{hsh}_{siz:06X}"
			return dir_entry.path, hsh

		t_start = time.perf_counter_ns()

		pth_gen = (data_dir / dir_map[bkt]).iter_on_suffix(
			*suffix_set, as_direntry=True
		)

		with ThreadPoolExecutor() as pool:
			pth_to_hsh = {
				pth[data_len:]: hsh for pth, hsh in pool.map(checksum, pth_gen)
			}

		t_stop = time.perf_counter_ns()

		print(
			f"scanned {len(pth_to_hsh)} files in {dir_map[bkt]}, took {int((t_stop - t_start) / 1e6)}ms"
		)

		return pth_to_hsh

	def fetch_date(self):
		cache_pth = self.memories_dir / "exif_date.tsv"

		if cache_pth.is_file():
			for hsh, dat in cache_pth.load():
				self.hsh_to_dat[hsh] = dat if dat else None

		hsh_to_pth = {
			hsh: pth
			for bkt, pth, hsh in self.iter_bkt_pth_hsh(exclude={"trash", "collision"})
		}

		new_hsh_set = hsh_to_pth.keys() - self.hsh_to_dat.keys()

		if new_hsh_set:
			print(len(new_hsh_set))
			info_lst = get_exif_date({hsh_to_pth[hsh] for hsh in new_hsh_set})
			for info in info_lst:
				pth = info["SourceFile"]
				dat = (
					datetime.datetime.strptime(
						info["CreateDate"], "%Y:%m:%d %H:%M:%S"
					).strftime("%Y%m%d_%H%M%S")
					if "CreateDate" in info
					else None
				)

				self.hsh_to_dat[self.pth_to_hsh[pth]] = dat

			cache_pth.save(sorted(self.hsh_to_dat.items()))

	def check_collision(self):
		"""on s'assure qu'il n'y a pas 2 fichier différents (hash différents), qui auraient en fait le même nom"""
		nam_to_hsh = collections.defaultdict(set)  # hsh to new name
		pth_to_nam = {}

		for pth in self.pth_to_hsh:
			nam = self.get_name(pth)
			nam_to_hsh[nam].add(self.pth_to_hsh[pth])
			if len(nam_to_hsh[nam]) > 1:
				raise ValueError(
					f"collisions on name {nam} for hashes: {','.join(nam_to_hsh[nam])}"
				)
			self.pth_to_nam[pth] = nam

		(self.memories_dir / "nam_to_hsh.json").save(nam_to_hsh, verbose=True)
		(self.memories_dir / "pth_to_nam.json").save(pth_to_nam, verbose=True)

	def move_into_bucket(self, src_pth, dst_bkt=None):
		# print(f"move_int_bucket({src_pth}, {dst_bkt})")
		dst_nam = self.get_name(src_pth, dst_bkt in {"trash"})

		pth_lst = src_pth.split("/")
		if dst_bkt:  # si le bucket n'est pas spécifié, on renomme sur place
			pth_lst[0] = dir_map[dst_bkt]
		pth_lst[-1] = dst_nam
		dst_pth = "/".join(pth_lst)

		if dst_pth != src_pth:
			if dst_pth in self.pth_to_hsh:
				if self.pth_to_hsh[dst_pth] == self.pth_to_hsh[src_pth]:
					print(f"{src_pth} already exists as {dst_pth}")
					os.unlink(src_pth)
				else:
					raise ValueError(f"{src_pth} collide with {dst_pth}")
			else:
				print(f"{src_pth} -> {dst_pth}")
				# os.makedirs("/".join(pth_lst[:-1]), exist_ok=True)
				shutil.move(src_pth, dst_pth)

		return dst_pth

	def rename_all(self):
		for bkt, pth_set in self.bkt_to_pth.items():
			for src_pth in pth_set:
				dst_bkt = self.hsh_to_bkt.get(self.pth_to_hsh[src_pth], "public")
				self.move_into_bucket(src_pth, dst_bkt)
