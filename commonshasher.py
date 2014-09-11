#! /usr/bin/python3
#
# Commonshashes calculates perceptual hashes on images from Wikimedia
# Commons, using a blockhash algorithm, and image information from a
# Wikimedia Commons database dump as input.
#
# Copyright 2014 Commons Machinery http://commonsmachinery.se/

"""
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import bz2
import argparse
import urllib.request, urllib.parse, urllib.error
import time
import shelve
import logging
import sys
import string
import tempfile
import subprocess
from lxml import etree


"""This sets up a custom User-Agent for our calls to the Wikimedia
   Commons API. """
class AppURLopener(urllib.request.FancyURLopener):
    version = "CommonsHasher-CommonsMachinery/0.01"

urllib.request._urlopener = AppURLopener()

lastapireq = None;  # time() of the last API request
APIBASE = "http://commons.wikimedia.org/w/api.php?format=xml&action=query&prop=imageinfo&iilimit=50&iiprop=sha1|url|thumbmime|extmetadata|archivename&iiurlwidth=1024&iilimit=1"

datafile = "commons.db"  # Local shelve storage
count = 0 # Number of filenames completed
log = logging.getLogger(sys.modules['__main__'].__file__)

def processfiles(filelist):
	global lastapireq, count
	""" Hold off on API reuqests so that we don't do more than
	one every six seconds (12 per minute) as the recommended best
	practice. """
	while lastapireq and (lastapireq + 6) > time.time():
		log.debug("Sleeping")
		time.sleep(1)
	lastapireq = time.time()

	apirequest = "%s&titles=%s" %  ( APIBASE, "|".join(filelist) )
	log.debug("Requesting %s" % apirequest)
	apidata = None
	sleeptimer = 1
	while not apidata:
		try:
			apidata = etree.parse(apirequest)
		except lxml.etree.XMLSyntaxError: # No data returned
			log.warning("No data returned from API, sleeping %d seconds" % sleeptimer)
			time.sleep(sleeptimer)
			if sleeptimer <= 128:
				sleeptimer *= 2

	log.debug("Opening %s" % datafile)
	store = shelve.open(datafile)

	for filename in filelist:
		log.debug("Traversing API output for %s" % filename)
		filedata = {}
		filedata['thumburl'] = apidata.find('.//page[@title="%s"]//ii' % filename).get('thumburl')
		filedata['identifier'] = apidata.find('.//page[@title="%s"]//ii' % filename).get('descriptionurl')
		filedata['sha1'] = apidata.find('.//page[@title="%s"]//ii' % filename).get('sha1')

		values = {"licenseurl": "LicenseUrl",
			"licenseshort": "LicenseShortName",
			"copyrighted": "Copyrighted",
			"artist": "Artist",
			"description": "ImageDescription"}
		for k in values:
			rawnode = apidata.find('.//page[@title="%s"]/ii//%s' % (filename, values[k]))
			if rawnode is not None:
				filedata[k] = rawnode.get('value')

		""" Check if the file has actually been updated since last time
		we retrieved it or not. We retrieve the file if we don't
		have it in the DB or if the sha1 differ from what we have. """
		if not filename in store or not 'sha1' in store[filename] or store[filename]['sha1'] != filedata['sha1']:
			tfile = tempfile.NamedTemporaryFile()
			log.debug("Retrieving %s to %s" % (filedata['thumburl'], tfile.name))
			urllib.request.urlretrieve(filedata['thumburl'], tfile.name)
		
			# TODO: Make this more intelligent :-)
			try:
				retval = subprocess.check_output(['../blockhash-python/blockhash.py', tfile.name], universal_newlines=True)
			except subprocess.CalledProcessError:
				log.debug("%s not supported by blockhash.py" % filename)
			else:
				hash = retval.partition(" ")[2]
				hash = hash.strip()
				log.debug("Calculated blockhash: %s" % hash)
				filedata['blockhash'] = hash
			tfile.close()

		store[filename] = filedata
		count += 1
		if count % 50 == 0:
			log.warning("Completed %d files", count)
	store.close()

def main():
	parser = argparse.ArgumentParser()

	parser.add_argument('-i', '--input', dest="file",
				help='A Commons dump file (bz2 compressed)')
	parser.add_argument('-d', '--data', dest="data",
				help='Local database file (default: commons.db)')
	parser.add_argument('-v', '--verbose', dest="verbose", action="count",
				default=0,
				help='Increase log verbosity')
	args = parser.parse_args()

	if not args.file:
		parser.print_help()
	if not args.data:
		datafile = args.data
	log.setLevel(max(3-args.verbose, 0)*10)

	log.debug("Opening %s" % args.file)
	reader = bz2.BZ2File(args.file, 'r')

	dump = etree.iterparse(reader, events=('end',))

	""" filelist holds the list of files to send in a single API call.
	This is limited to 50 for unregistered bots, but we can go up to
	500 if we register. """
	filelist = []
	filelist_max = 50

	for event, elem in dump:
		if elem.tag == '{http://www.mediawiki.org/xml/export-0.9/}page' and elem.findtext('.//{http://www.mediawiki.org/xml/export-0.9/}title').startswith('File:'):
			filename = elem.findtext('.//{http://www.mediawiki.org/xml/export-0.9/}title')
			""" We need to query the API for information about all
			files, since there's no way to tell whether the file
			has changed since last time or not in the dump. """
			filelist.append(filename)

			elem.clear()
			while elem.getprevious() is not None:
				del elem.getparent()[0]
			if len(filelist) >= filelist_max:
				processfiles(filelist)
				filelist[:] = []
	processfiles(filelist)

if __name__ == "__main__":
	logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
		format="%(asctime)-15s %(levelname)s: %(message)s")
	logging.info("Starting work")
	main()
	logging.info("Shutting down")
	logging.shutdown()
