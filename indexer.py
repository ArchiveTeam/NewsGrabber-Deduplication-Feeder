import gzip
import hashlib
import os
import re
import time

import redis

from session import Session


class Indexer(object):
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    ex = 30*24*3600

    def __init__(self):
        self.items = {}

    def run(self):
        while True:
            self.run_indexing()
            time.sleep(300)

    def run_indexing(self):
        r = Session.get('https://archive.org/metamgr.php?f=exportIDs' \
                        '&w_collection=archiveteam_newssites*')
        if not r:
            return
        for line in r.text.splitlines():
            line = line.strip()
            if line in self.indexed:
                continue
            if len(line.split('_')[-1]) >= 14 and line.count('_') == 2:
                if not line in self.items:
                    self.items[line] = Item(line)
                    self.items[line].run()

    @property
    def indexed(self):
        if not os.path.isfile('indexed'):
            return []
        if not hasattr(self, '_indexed'):
            finished = []
            with open('indexed', 'r') as f:
                for line in f:
                    line = line.strip()
                    finished.append(line)
            self._finished = finished
        return self._finished
            

    @classmethod
    def add_record(cls, url, date, hash_, type_):
        #print hash_ + ';' + re.sub('^https?://', '', url)
        hashed = hashlib.sha256(hash_ + ';' + re.sub('^https?://', '', url)) \
                 .hexdigest()
        if type_ == 'warc/revisit':
            cls.renew_record(url, date, hashed)
        else:
            cls.new_record(url, date, hashed)

    @classmethod
    def new_record(cls, url, date, hashed):
        cls.r.set(hashed, date+';'+url, ex=cls.ex)

    @classmethod
    def renew_record(cls, url, date, hashed):
        if not cls.r.get(hashed):
            return
        cls.r.expire(hashed, cls.ex)


class Item(object):
    def __init__(self, identifier):
        self.identifier = identifier.strip()

    def run(self):
        if not self.cdx:
            return self._fail_index()
        with gzip.open(self.cdx, 'rb') as f:
            for line in f:
                data = line.strip().split(' ')
                Indexer.add_record(data[2], data[1], data[5], data[3])
            return self._finish_index()

    def _finish_index(self):
        with open('indexed', 'a') as f:
            f.write(self.identifier + '\n')
        os.remove(self.cdx)

    def _fail_index(self):
        pass

    @property
    def cdx_url(self):
        if not hasattr(self, '_cdx_url'):
            print 'https://archive.org/download/{i}' \
                .format(i=self.identifier)
            r = Session.get('https://archive.org/download/{i}' \
                .format(i=self.identifier))
            if self.identifier + '.cdx.gz' in r.text:
                self._cdx_url = 'https://archive.org/download/{i}/{i}.cdx.gz' \
                                .format(i=self.identifier)
            else:
                return None
        return self._cdx_url

    @property
    def cdx(self):
        if not hasattr(self, '_cdx'):
            if self.cdx_url:
                filename = self.identifier + '.cdx.gz'
                r = Session.get(self.cdx_url)
                print r, self.cdx_url
                if r:
                    with open(filename, 'wb') as f:
                        f.write(r.content)
                    self._cdx = filename
                else:
                    del r
                    return
                del r
            else:
                return
        return self._cdx