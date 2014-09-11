commonshasher
=============

This is a utility script that loads a database dump from Wikimedia Commons
and uses it as a starting point to request metadata and thumbnails from
images on Commons. It calls upon and runs a Blockhash algorithm
(see http://blockhash.io) on this data and stores in a database file.

Usage
-----
This script requires Python 3 to run. It also requires a bzip2'ed copy of
a recent Commons database dump (a file such as
commonswiki-20140823-pages-articles.xml.bz2). You can optionally specify
the name of the local database file, which will otherwise default to commons.db.

Use `commonshasher.py -i [commons database file]` to run.
Each additional `-v` will increase the verbosity level. Use `-h` to get
more detailed help.

Contact
-------
The script is developed by Commons Machinery, and you can reach out to the
development team by writing dev@commonsmachinery.se


Planned work
------------
* There's no technical need to use a database dump, the script should just run on data from Commons directly to also capture information that comes between dumps.
* The script should output media information consistent with the Data Package Format at https://github.com/commonsmachinery/catalog/blob/master/doc/datapackage.md


License
-------
Copyright 2014 Commons Machinery http://commonsmachinery.se/

Distributed under the GNU General Public License v3.0 or any later version
(see the file LICENSE for details).

