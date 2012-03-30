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


class CanvecExtractor:
	"""
		Provides a method for extracting CanVec files matching a search string 
		and creating an PostGIS-compatible SQL file to create and populate a table 
		containing the data.
	"""

	def extract(self, search, tableName, schemaName, sqlFile, canvecDir, tmpDir):
		"""
			Perform the extraction and create the SQL file.
			
			Named Arguments:
			search -- A search string to match the name of the file (the file's data is identifiable by its name. For example, metric contours would match 'FO_1030009').
			tableName -- The name of the table to create/populate. The table will be dropped!
			schemaName -- The schema name of the table.
			sqlFile -- A file to write the SQL to.
			canvecDir -- The name of the Canvec directory. Required.
			tmpDir -- The temporary directory. If the directory does not exist, it will be created.
		"""
		# Check parameters
		if sqlFile is None:
			raise "No SQL file."
		if search is None:
			raise "No search string."
		if schemaName is None:
			raise "No schema name."
		if tableName is None:
			raise "No table name."
		if canvecDir is None:
			raise "No canvec dir."
		if tmpDir is None:
			raise "No temp dir."
		# Check if the cenvec dir exists.
		if not self._dirExists(canvecDir):
			raise "The canvec dire (%s) doesn't seem to exist." % canvecDir
		# Create the tmp dir if it doesn't exist.
		if not self._dirExists(tmpDir):
			self._createDir(tmpDir)
		self.searchRe = re.compile(search)
		self.schemaName = schemaName
		self.tableName = tableName
		self.canvecDir = canvecDir
		self.tmpDir = tmpDir
		self.sqlFile = sqlFile
		# Start!
		archives = self._getArchives()
		shapefiles = self._extractShapefiles(archives)
		self._createSql(shapefiles)

	# Returns true if a directory exists. False otherwise.
	def _dirExists(self, dir):
		# Check and create the tmpdir
		return os.access(dir, os.R_OK|os.W_OK)
		
	# Create a directory.
	def _createDir(self, dir):
		os.makedirs(dir)

	# Extracts the shapefiles matching the given re from the archives stored in the file list.
	# Returns a list of the shapefiles (shp) in the archive.
	def _extractShapefiles(self, fileList):
		shpfiles = list()
		shpmatch = re.compile('\.shp$')
		# Iterate through the list of archives.
		for f in fileList:
			try:
				# Open the zip file and read the entries.
				z = ZipFile(f)
				entries = z.namelist()
				# Iterate through the entries. If an entry matches the search string, extract
				# it to the tmp folder.
				for e in entries:
					if self.searchRe.search(e):
						z.extract(e, self.tmpDir)
						# If the entry is a shapefile, add it to the shp list.
						if shpmatch.search(e):
							shpfiles.append(e)
			except:
				print "Failed to open %s." % f
			finally:
				try:
					z.close()
				except:
					pass
		return shpfiles
	
	# Prints the SQL containing the DDL and data from all the shape files in the given list.
	# This could be huge.
	def _createSql(self, shapefiles):
		# A command to create/populate the table, on the first file.
		cmdA = "shp2pgsql -s 4326 -d -I %s/%s %s.%s > %s"
		# A command to append data to an existing table.
		cmdB = "shp2pgsql -s 4326 -a %s/%s %s.%s >> %s"
		cmd = ''
		# Iterate over the list of shapefiles, calling shp2pgsql on each one.
		# This outputs the sql.
		for i in range(0, len(shapefiles)):
			if i > 0:
				cmd = cmdB
			else:
				cmd = cmdA
			cmd = cmd % (self.tmpDir, shapefiles[i], self.schemaName, self.tableName, self.sqlFile)
			os.system(cmd)
			
	# Returns a list of all the zip archives under the given directory.
	# Search is recursive. If no archive is found, the empty list is returned.
	def _getArchives(self):
		fileList = list()
		# A regex for finding the zip files.
		match = re.compile('\.zip$')
		# Get a list of all files under the canvec directory.
		files = os.walk(self.canvecDir)
		# Iterate over the file list and add the zip files to the 
		# file list. It doesn't really matter if we pick up non-canvec zips, 
		# because we're only going to extract files that match the search
		# string.
		for f in files:
			dirpath = f[0]
			for l in f[2]:
				if match.search(l):
					fileList.append("%s/%s" % (dirpath, l))
		return fileList
		
# If run from the command line, pretend to be a program.
if __name__ == "__main__":
	import sys
	args = sys.argv[1:]
	if len(args) < 6:
		print "Usage: python canvec.py <search> <schemaname> <tablename> <canvecdir> <tmpdir>\n"
		print " search     - A search term to use against the shapefile names."
		print " schemaname - The name of the database schema."
		print " tablename  - The name of the database table."
		print " sqlfile    - A file to write the SQL to."
		print " canvecdir  - The directory where the canvec archives are located."
		print " tmpdir     - A temporary directory for extracting the canvec files."
	else:
		c = CanvecExtractor()
		c.extract(args[0], args[1], args[2], args[3], args[4], args[5])