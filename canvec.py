#######################################################################################################
# Copyright (C) 2012 Rob Skelly                                                                       #
#                                                                                                     #
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software       #
# and associated documentation files (the "Software"), to deal in the Software without restriction,   #
# including without limitation the rights to use, copy, modify, merge, publish, distribute,           #
# sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is       #
# furnished to do so, subject to the following conditions:                                            #
#                                                                                                     #
# The above copyright notice and this permission notice shall be included in all copies               #
# or substantial portions of the Software.                                                            #
#                                                                                                     #
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING       #
# BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND          #
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,        #
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,      #
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.             #
#######################################################################################################

from zipfile import ZipFile
import os
import re
import subprocess
import gzip
import zlib

class Base(object):
	"""
	Base class. Provides convenient methods.
	"""
	
	def _dirExists(self, folder):
		"""Returns true if a directory exists. False otherwise."""
		return os.access(folder, os.R_OK|os.W_OK)

	def _deleteFile(self, filename):
		"""
		Deletes a file. Swallows the error if there is one.

		Arguments:
			file -- The path to the file.close
		"""
		try:
			os.unlink(filename)
		except:
			pass

	def _createDir(self, folder):
		"""
		Create a directory. Will attempt to create any parent
		directories.

		Arguments:
			folder -- The name of the directory.
		"""
		os.makedirs(folder)


class ShapefileList(Base):
	"""
	Provides a means of iterating over a list of shapefiles contained in a
	heirarchical list of zip files. Extracts shapefiles in batches of configurable
	size, matching the provided pattern. And disposes of each temporary file
	once it has been passed.
	"""
	
	def __init__(self, pattern, folder, tmpDir = '/tmp', batchSize = 50):
		"""
		Initializes a ShapefileList object.
		
		Arguments:
			pattern -- A regular expression matching the canvec feature code of a shapefile.
			folder -- The data directory where the archives are stored.
			tmpDir -- The temporary directory where archives are extracted.
			batchSize -- The number of shapefiles to be extracted at a time. This many are extracted, and they're deleted as the list is consumed.
		"""
		if not pattern:
			raise Exception('The pattern parameter is required.')
		if not folder or not self._dirExists(folder):
			raise Exception('The data directory, {0}, does not exist or is not accessible.'.format(folder))
		if not tmpDir or not self._dirExists(tmpDir):
			raise Exception('The temporary directory, {0}, does not exist or is not accessible.'.format(tmpDir))
		self.pattern = pattern
		self.folder = folder
		self.tmpDir = tmpDir
		self.batchSize = batchSize
		self.shpList = None
		self.zipList = None
		self._currentShp = None
		self._triggerCleanup = False
		
	def _loadZipList(self):
		"""
		Returns a list of all the zip archives under the given directory.
		Search is recursive. If no archive is found, the empty list is returned.
		"""
		fileList = list()
		# A regex for finding the zip files.
		match = re.compile('\.zip$')
		# Get a list of all files under the canvec directory.
		files = os.walk(self.folder)
		# Iterate over the file list and add the zip files to the 
		# file list. It doesn't really matter if we pick up non-canvec zips, 
		# because we're only going to extract files that match the search
		# string.
		for f in files:
			dirpath = f[0]
			for l in f[2]:
				if match.search(l):
					fileList.append("{0}/{1}".format(dirpath, l))
		self.zipList = fileList
		self.zipIndex = 0
		
	def _loadShpList(self):
		"""
		Loads (extracts) a batch of shapefiles from an archive. Only those that match
		the given pattern are extracted.
		"""
		shpFiles = list()
		tmpFiles = list()
		shpmatch = re.compile('\.shp$')
		searchRe = re.compile(self.pattern)
		# Iterate through the list of archives.
		for i in range(self.zipIndex, len(self.zipList)):
			f = self.zipList[i]
			try:
				# Open the zip file and read the entries.
				z = ZipFile(f)
				entries = z.namelist()
				# Iterate through the entries. If an entry matches the search string, extract
				# it to the tmp folder.
				for e in entries:
					if searchRe.search(e):
						z.extract(e, self.tmpDir)
						# Add to the list of extracted files.
						tmpFiles.append(e)
						# If the entry is a shapefile, add it to the shp list.
						if shpmatch.search(e):
							shpFiles.append(e)
			except Exception, e:
				print "Failed to open {0}: {1}.".format(f, e.__str__())
			finally:
				try:
					z.close()
				except:
					pass
			# Set the current zip index for next batch load.
			self.zipIndex = i + 1
			# If we have enough shapefiles, stop.
			if len(shpFiles) >= self.batchSize:
				break
		# Save the list of shapefiles.
		self.shpList = shpFiles
		self.tmpFiles = tmpFiles

	def next(self):
		"""
		Returns the next shapefile in the list.
		"""
		if self._triggerCleanup:
			try:
				for f in self._cleanupFiles:
					self._deleteFile(os.path.join(self.tmpDir, f))
			except:
				pass
			self._triggerCleanup = False			
		if self.shpList and len(self.shpList) == 1:
			self._triggerCleanup = True
			self._cleanupFiles = self.tmpFiles
		if not self.hasNext():
			return False
		# Get the next shapefile name.
		self._currentShp = self.shpList.pop(0)
		# Return the entire path.
		return self._currentShp

	def hasNext(self):
		"""
		Returns true if there's at least one more shapefile to come.
		"""
		if not self.zipList:
			self._loadZipList()
		if not self.shpList or len(self.shpList) == 0:
			self._loadShpList()
		if not self.shpList or len(self.shpList) == 0:
			return False
		return True

	def cleanup(self):
		"""
		Cleans up the extracted files.
		"""
		try:
			for f in self.tmpFiles:
				self._deleteFile(os.path.join(self.tmpDir, f))
		except:
			pass
		self.zipList = None
		self.shpList = None

	def __del__(self):
		self.cleanup()
		
class ZipWriter(object):
	
	def __init__(self, filename):
		self._h = gzip.open(filename, "wb")
		
	def flush(self):
		self._h.flush()
		
	def seek(self, offset, whence = 0):
		self._h.seek(offset, whence)
		
	def close(self):
		self._h.close()
		
	def read(self, size):
		return zlib.decompress(self._h.read(size))
	
	def write(self, value):
		value = zlib.compress(value, 9)
		self._h.write(value)
		
	def tell(self):
		self._h.tell()
		
	def truncate(self, size):
		self._h.truncate(size)
		
	def writelines(self, lines):
		for l in lines:
			self._h.write(zlib.compress(l))
			
	def __del__(self):
		try:
			self._h.close();
		except:
			pass
		
	@property
	def closed(self):
		return self._h.closed
	
	def fileno(self):
		return self._h.fileno()
	
class CanvecExtractor(Base):
	"""
	Provides a method for extracting CanVec files matching a search string 
	and creating an PostGIS-compatible SQL file to create and populate a table 
	containing the data.
	"""

	def __init__(self):
		self.encoding = 'utf-8'
		self.srid = 4326
		
	def extract(self, pattern, outFile, canvecDir, tableName, schemaName = 'public', tmpDir = '/tmp'):
		"""
		Perform the extraction and create the SQL file.
		
		Arguments:
			pattern     -- A search string to match the name of the file (the file's data is identifiable by its name. For example, metric contours would match 'FO_1030009').
			outFile    -- A file to write the SQL to.
			canvecDir  -- The name of the Canvec directory. Required.
			tableName  -- The name of the table to create/populate. The table will be dropped!
			schemaName -- The schema name of the table. Defaults to 'public'
			tmpDir     -- The temporary directory. If the directory does not exist, it will be created. Defaults to '/tmp'
		"""
		# Check parameters
		if outFile is None:
			raise Exception("No output file.")
		if pattern is None:
			raise Exception("No pattern string.")
		if schemaName is None:
			raise Exception("No schema name.")
		if tableName is None:
			raise Exception("No table name.")
		if canvecDir is None:
			raise Exception("No canvec dir.")
		if tmpDir is None:
			raise Exception("No temp dir.")
		# Check if the cenvec dir exists.
		if not self._dirExists(canvecDir):
			raise Exception("The canvec dir ({0}) doesn't seem to exist.".format(canvecDir))
		# Create the tmp dir if it doesn't exist.
		if not self._dirExists(tmpDir):
			self._createDir(tmpDir)

		self._shapeFileList = ShapefileList(pattern, canvecDir, tmpDir)

		self.outFile = outFile
		self.schemaName = schemaName
		self.tableName = tableName
		self.tmpDir = tmpDir
		
		self._createSql()
		
	def _createSql(self):
		"""
		Prints the SQL containing the DDL and data from all the shape files in the given list.
		This could be huge.
		"""		
		outName = self.outFile[0:self.outFile.rfind('.')]
		outExt = self.outFile[self.outFile.rfind('.') + 1:]
		fileNum = 1
		maxSqlSize = 100 * 1024 * 1024
		end = False
		# If true, use the append operator, otherwise clobber
		append = False
		# If true, use the drop/create switch, otherwise append.
		create = True
		# Iterate over the list of shapefiles, calling shp2pgsql on each one. This outputs the sql.
		while True:
			shapefile = self._shapeFileList.next()
			if not shapefile:
				print "Done"
				return
			# If this is the last file, add the index operator onto the command.
			if not self._shapeFileList.hasNext():
				end = True
			# Create the sql file.
			outFile = "{0}.{1}.{2}".format(outName, fileNum, outExt)
			if not append:
				if outExt == 'gz':
					raise Exception('Zipping doesn\'t work right now.')
					h = gzip.open(outFile, 'wb')
				else:
					h = open(outFile, 'wb')
			# First part of command.
			args = ["shp2pgsql"]
			# Add the srid
			args.append("-s {0}".format(self.srid))
			# Add the create/append switch.
			if create:
				args.append("-d")
			else:
				args.append("-a")
			# Add the index switch; only happens at the end.
			if end:
				args.append("-I")
			# Add the encoding
			args.append('-W {0}'.format(self.encoding))
			# Add the shapefile name.
			args.append(shapefile)
			# Add the schema/table names.
			args.append("{0}.{1}".format(self.schemaName, self.tableName))
			# Call the command to output the file.
			curDir = os.path.abspath(os.curdir)
			os.chdir(self.tmpDir);
			p = subprocess.Popen(args, stdout=h)
			p.wait()
			os.chdir(curDir);
			# If the sql file is too large, rotate it.
			if os.path.getsize(outFile) > maxSqlSize:
				h.close()
				fileNum = fileNum + 1
				append = False
			else:
				append = True
			# Only call with the drop switch on the first iteration.
			create = False
		
if __name__ == "__main__":
	import sys
	args = sys.argv[1:]
	if len(args) < 4:
		print "Usage: python canvec.py <pattern> <outfile> <canvecdir> <tablename> [schemaname] [tmpdir]\n"
		print " pattern     - A pattern term to use against the shapefile names."
		print " outfile    - A file to write the SQL to. If it ends with .gz, it's a gzipped file."
		print " canvecdir  - The directory where the canvec archives are located."
		print " tablename  - The name of the database table."
		print " schemaname - The name of the database schema. Defaults to 'public'"
		print " tmpdir     - A temporary directory for extracting the canvec files. Defaults to '/tmp'"
	else:
		c = CanvecExtractor()
		c.extract(*args)
