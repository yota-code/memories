#!/usr/bin/env python3

import base64
import collections
import datetime
import os
import sys

import blake3
import itertools
import subprocess

from cc_pathlib import Path

def get_datetime(pth) :
	ret = subprocess.run(["exiftool", str(pth)], stdout=subprocess.PIPE)
	for line in ret.stdout.decode('utf8', errors="replace").splitlines() :
		key, sep, value = line.partition(':')
		if 'Create Date' == key.strip() :
			try :
				return datetime.datetime.strptime(value.strip(), "%Y:%m:%d %H:%M:%S").strftime("%Y%m%d_%H%M%S")
			except ValueError :
				pass
	return None
	
def get_checksum(pth) :
	buf = pth.read_bytes()
	hsh = blake3.blake3(buf).digest()
	b32 = base64.b32encode(hsh).decode('ascii')
	return b32, len(buf)

def are_files_identical(a_pth, b_pth) :

	a_size = a_pth.stat().st_size
	b_size = b_pth.stat().st_size

	if a_size != b_size :
		return False

	if a_size < 2**28 : # 256 Mo
		return a_pth.read_bytes() == b_pth.read_bytes()

	with (
		a_pth.open('rb') as a_fid,
		b_pth.open('rb') as b_fid
	) :
		while True :
			a_bin = a_fid.read(2**26) # 64 Mo
			b_bin = b_fid.read(2**26)
			if a_bin != b_bin :
				return False
			if len(a_bin) < 2**26 :
				break
		return True

# ext_map = {
# 	'.jpg' : [".jpeg"],
# 	'.png' : [".png",],
# 	# '.gif' : [".gif", ".GIF"],
# 	'.mp4' : [".mp4",],
# 	'.mov' : [".mov",],
# }

for k, v_lst in ext_map.items() :
	for v in v_lst :
		rev_map[v] = k

class RenamePhoto() :
	
	proc_set = {'.jpg', '.png', '.mp4', '.mov'}
	repl_map = {
		'.jpeg': '.jpg'
	}

	def __init__(self, root_dir:Path, dry_run:bool) :

		self.root_dir = Path(root_dir).resolve()
		assert(self.root_dir.is_dir() or self.root_dir.is_mount())

		self.dry_run = dry_run

		self.log_pth = Path(os.environ['MEMORIES_root_DIR']) / "log" / f"rename_photo.{datetime.datetime.now():%Y%m%d_%H%M%S}.log"

	def write_log(self, s) :
		if self.dry_run :
			print(s)
		else :
			with self.log_pth.open('at') as fid :
				fid.write(s + '\n')

	def process_dir(self, base_dir, do_recurse=False) :
		print(f"process_dir({work_dir}, {do_recurse})")

		work_dir = Path(work_dir).resolve()

		q_set = set()
		d_lst = [base_dir,]

		while d_lst :

			base_dir = d_lst.pop(0)

			for src_pth in base_dir :
				if src_pth.is_file() :
					src_ext = src_pth.suffix.lower()
					dst_ext = self.repl_map.get(src_ext, src_ext)
					if dst_ext in self.proc_set :
						try :
							self.process_file(src_pth, dst_ext)
						except ValueError :
							q_lst.add((src_pth, dst_ext))
				
				elif src_pth.is_dir() :
					if do_recurse :
						d_lst.append(src_pth)
					
	def solve_duplicates(self, pth_set) :
		pass

	def process_file(self, src_pth, dst_ext) :

		dat = get_datetime(src_pth)
		if dat is None :
			# la date n'est pas dans le fichier, on arrête là, on traitera le problème après
			raise ValueError
	
		hsh, siz = get_checksum(src_pth)

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
	
	u = RenamePhoto(os.environ['SHARED_root_DIR'], False)
	u.process_dir(sys.argv[1], True)

	# import argparse
	
	# pa = argparse.ArgumentParser(prog='RenamePics', description='give standard names for images based on exif dates and checksum')

	# pa.add_argument('arg_lst', nargs='+', type=Path)
	# pa.add_argument('-n', '--dry-run', action='store_true')
	# pa.add_argument('-r', '--recurse', action='store_true')

	# ap = pa.parse_args()

	# u = RenamePics(dry_run=ap.dry_run)

	# for pth in ap.arg_lst :
	# 	pth = pth.resolve()
	# 	if pth.is_dir() :
	# 		u.process_dir(pth, do_recurse=ap.recurse)
	
