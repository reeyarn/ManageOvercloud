This class replaces local file IO functions such as os.path.isfile(), os.path.isdir(), open(...).read(), open(...).write()  and operates flexibles on local storage and dropbox cloud.

The application scenario is: when processing the SEC EDGAR filings with the overall size at the scale of TBs, it is impossible to store everything on the Computing Node. Hosting it on some storage cloud with fast Read/Write speed (such as Drpobox) could be a solution.

Syncing from Dropbox cloud alone is much faster than Downloading from EDGAR. In addition, text files are stored as gzip file, usually only 10--40% of size. With parallel downloading, when running in computing nodes with 64+ cores, it feels like reading from the local filesystem.

