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
	
	def _dirExists(self, dir):
		"""Returns true if a directory exists. False otherwise."""
		return os.access(dir, os.R_OK|os.W_OK)

	def _deleteFile(self, file):
		"""
		Deletes a file. Swallows the error if there is one.

		Arguments:
			file -- The path to the file.close
		"""
		try:
			os.unlink(file)
		except:
			pass

	def _createDir(self, dir):
		"""
		Create a directory. Will attempt to create any parent
		directories.

		Arguments:
			dir -- The name of the directory.
		"""
		os.makedirs(dir)


class ShapefileList(Base):
	"""
	Provides a means of iterating over a list of shapefiles contained in a
	heirarchical list of zip files. Extracts shapefiles in batches of configurable
	size, matching the provided pattern. And disposes of each temporary file
	once it has been passed.
	"""
	
	def __init__(self, pattern, dir, tmpDir = '/tmp', batchSize = 50):
		"""
		Initializes a ShapefileList object.
		
		Arguments:
			pattern -- A regular expression matching the canvec feature code of a shapefile.
			dir -- The data directory where the archives are stored.
			tmpDir -- The temporary directory where archives are extracted.
			batchSize -- The number of shapefiles to be extracted at a time. This many are extracted, and they're deleted as the list is consumed.
		"""
		if not pattern:
			raise Exception('The pattern parameter is required.')
		if not dir or not self._dirExists(dir):
			raise Exception('The data directory, {0}, does not exist or is not accessible.'.format(dir))
		if not tmpDir or not self._dirExists(tmpDir):
			raise Exception('The temporary directory, {0}, does not exist or is not accessible.'.format(tmpDir))
		self.pattern = pattern
		self.dir = dir
		self.tmpDir = tmpDir
		self.batchSize = batchSize
		self.shpList = None
		self.zipList = None
		self._currentShp = None
		
	def _loadZipList(self):
		"""
		Returns a list of all the zip archives under the given directory.
		Search is recursive. If no archive is found, the empty list is returned.
		"""
		fileList = list()
		# A regex for finding the zip files.
		match = re.compile('\.zip$')
		# Get a list of all files under the canvec directory.
		files = os.walk(self.dir)
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
		self.shpList = None
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
		if not self.zipList:
			self._loadZipList()
		if not self.shpList or len(self.shpList) == 0:
			self._loadShpList()
		if not self.shpList or len(self.shpList) == 0:
			return None
		# Delete the previous current file.
		self._deleteFile(self._currentShp)
		# Get the next shapefile name.
		self._currentShp = self.shpList.pop(0)
		# Return the entire path.
		return os.path.join(self.tmpDir, self._currentShp)

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
			for f in self.shpList:
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
		self.currentShp = None
		self.shpList = None

	def __del__(self):
		self.cleanup()
		
class CanvecExtractor(Base):
	"""
	Provides a method for extracting CanVec files matching a search string 
	and creating an PostGIS-compatible SQL file to create and populate a table 
	containing the data.
	"""

	def __init__(self):
		pass
		
	def extract(self, pattern, sqlFile, canvecDir, tableName, schemaName = 'public', tmpDir = '/tmp'):
		"""
		Perform the extraction and create the SQL file.
		
		Arguments:
			pattern     -- A search string to match the name of the file (the file's data is identifiable by its name. For example, metric contours would match 'FO_1030009').
			sqlFile    -- A file to write the SQL to.
			canvecDir  -- The name of the Canvec directory. Required.
			tableName  -- The name of the table to create/populate. The table will be dropped!
			schemaName -- The schema name of the table. Defaults to 'public'
			tmpDir     -- The temporary directory. If the directory does not exist, it will be created. Defaults to '/tmp'
		"""
		# Check parameters
		if sqlFile is None:
			raise Exception("No SQL file.")
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

		self.sqlFile = sqlFile
		self.schemaName = schemaName
		self.tableName = tableName

		self._createSql()
		
	def _createSql(self):
		"""
		Prints the SQL containing the DDL and data from all the shape files in the given list.
		This could be huge.
		"""		
		sqlName = self.sqlFile[0:self.sqlFile.rfind('.')]
		sqlExt = self.sqlFile[self.sqlFile.rfind('.')+1:]
		fileNum = 1
		maxSqlSize = 100 * 1024 * 1024
		end = False
		cmd = "shp2pgsql -s 4326 {5} {6} {0} {1}.{2} {4} {3}"
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
			sqlFile = "{0}.{1}.{2}".format(sqlName, fileNum, sqlExt)
			# Call the command to output the file.
			os.system(cmd.format(shapefile, self.schemaName, self.tableName, sqlFile, '>>' if append else '>', '-d' if create else '-a', '-I' if end else ''))
			# If the sql file is too large, rotate it.
			if os.path.getsize(sqlFile) > maxSqlSize:
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
		print "Usage: python canvec.py <pattern> <sqlfile> <canvecdir> <tablename> [schemaname] [tmpdir]\n"
		print " pattern     - A pattern term to use against the shapefile names."
		print " sqlfile    - A file to write the SQL to."
		print " canvecdir  - The directory where the canvec archives are located."
		print " tablename  - The name of the database table."
		print " schemaname - The name of the database schema. Defaults to 'public'"
		print " tmpdir     - A temporary directory for extracting the canvec files. Defaults to '/tmp'"
	else:
		c = CanvecExtractor()
		c.extract(*args)