#!/usr/bin/env python3

import base64
import datetime
import enum
import functools
import logging
import mmap
import os
import multiprocessing
import re
import sys
import subprocess

import xxhash

from cc_pathlib import Path

def setup_logger(name, log_file, level=logging.INFO):
	"""To setup as many loggers as you want"""

	handler = logging.FileHandler(log_file)        

	logger = logging.getLogger(name)
	logger.setLevel(level)
	logger.addHandler(handler)
	logger.addHandler(logging.StreamHandler(sys.stdout))

	return logger

class RenameStatus(enum.Enum) :
	ALREADY = 0
	RENAMED = 1
	DUPLICATE = 2

class MemoriesPhoto() :
	# and videos
	
	hashed_res = re.compile(r'(([0-9]{8}_[0-9]{6}\.[A-Z2-7]{6})|(\.[A-Z2-7]{16}))$')

	suffix_set = {'.jpg', '.jpeg', '.png', '.mp4', '.mov'}
	replace_map = {
		'.jpeg': '.jpg'
	}

	def __init__(self) :
		self.base_dir = Path(os.environ['MEMORIES_photo_DIR']).resolve()
		assert (self.base_dir / ".memories").is_dir()

		self.rename_log = setup_logger("rename", self.base_dir / ".memories" / "rename.log")

	def get_hash(self, pth) :
		with pth.open("rb") as fid:
			with mmap.mmap(fid.fileno(), 0, prot=mmap.PROT_READ) as mmp :
				xx3 = xxhash.xxh3_128()
				xx3.update(mmp)
				hsh = xx3.digest()
				b32 = base64.b32encode(hsh).decode('ascii')[:24]
		return b32

	def get_datetime(self, pth) :
		""" utilise exiftool pour trouver un date dans les métadonnées """
		""" TAG

		Extract information for the specified tag (eg. -CreateDate)."""
		ret = subprocess.run(["exiftool", str(pth)], stdout=subprocess.PIPE)
		for line in ret.stdout.decode('utf8', errors="replace").splitlines() :
			key, sep, value = line.partition(':')
			if 'Create Date' == key.strip() :
				try :
					return datetime.datetime.strptime(value.strip(), "%Y:%m:%d %H:%M:%S").strftime("%Y%m%d_%H%M%S")
				except ValueError :
					pass
		return None

	def rename_file(self, cwd, src_pth) :
		# print(f"rename_file({cwd}, {src_pth})")

		# if self.hashed_res.search(src_pth.stem) is not None :
		# 	# on dirait bien que le boulot est déjà fait
		# 	return

		hsh = self.get_hash(src_pth)
		dat = self.get_datetime(src_pth)
		suf = self.replace_map.get(src_pth.suffix.lower(), src_pth.suffix.lower())

		if dat is None :
			dst_pth = src_pth.parent / f"{src_pth.fname}.{hsh[:16]}{suf}"
		else :
			dst_pth = src_pth.parent / f"{dat}.{hsh[:6]}{suf}"

		if dst_pth == src_pth :
			print(f"{dst_pth.relative_to(cwd)} DUPLICATE !!")
			return RenameStatus.DUPLICATE, src_pth.relative_to(cwd), dst_pth.relative_to(cwd)
		else :
			print(f"{src_pth.relative_to(cwd)} -> {dst_pth.relative_to(cwd)}")
			return RenameStatus.RENAMED, src_pth.relative_to(cwd), dst_pth.relative_to(cwd)

	def rename_dir(self, cwd=None, dry_run=True) :
		cwd = (Path(cwd) if cwd is not None else Path()).resolve()
		print(f"rename_dir({cwd})")

		p_gen = cwd.iter_on_suffix(* self.suffix_set)
		p_fct = functools.partial(MemoriesPhoto.rename_file, self, cwd)

		with multiprocessing.Pool() as pool :
			for ret in pool.map(p_fct, p_gen) :
				self.rename_log.info(f"{ret[0].name} :: {cwd.relative_to(self.base_dir)} / {ret[1]} -> {ret[2]}")

	def _process_dir(self, ) :

		q_set = set()
		d_lst = [self.base_dir,]

		while d_lst :
			self.base_dir = d_lst.pop(0)
			for src_pth in self.base_dir :
				if src_pth.is_file() :
					src_ext = src_pth.suffix.lower()
					dst_ext = self.repl_map.get(src_ext, src_ext)
					if dst_ext in self.proc_set :
						try :
							self.process_file(src_pth, dst_ext)
						except ValueError :
							q_set.add((src_pth, dst_ext))
				
				elif src_pth.is_dir() :
					if self.do_recurse :
						d_lst.append(src_pth)
					
	def process_file(self, src_pth, dst_ext) :

		dat = get_datetime(src_pth)
		hsh, siz = get_checksum(src_pth)

		if dat is None :
			dst_name = f"{src_pth.fname}_{hsh}{dst_ext}"
			if src_pth.name.endswith(f"_{hsh}{dst_ext}") :
				# on renomme pas ! ça a déjà été fait
				return
		else :
			dst_name = f"{dat}_{hsh[:6]}{dst_ext}"
			if dst_name == src_pth.name :
				# le nom qu'on veut donner, c'est le nom qu'il a déjà, pas besoin de le renommer
				return
		
		dst_pth = src_pth.parent / dst_name

		if dst_pth.is_file() :
			# il y a dejà un fichier au même endroit et qui a le même nom !
			if are_files_identical(src_pth, dst_pth) :
				# le fichier traité existe déjà sous un autre nom, on supprime ce premier
				if not self.dry_run :
					src_pth.unlink()
				self.write_log(f'{src_pth.relative_to(self.root_dir)} -X')
			else :
				# sinon, on lève une erreur, Houston, on a un problème
				self.write_log(f'{col_pth.relative_to(self.root_dir)} ?! {dst_pth.relative_to(self.root_dir)}')
				raise FileExistsError
		else :
			# il n'y a pas de fichier, on renomme
			if not self.dry_run :
				src_pth.rename(dst_pth)
			self.write_log(f'{src_pth.relative_to(self.root_dir)} -> {dst_pth.relative_to(self.root_dir)}')


if __name__ == '__main__' :

	import sys

	u = Memories(Path(sys.argv[1]))
	u.rename(Path(sys.argv[2]))