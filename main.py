import sys

from indexer import Indexer
from session import Session

def main():
    Session()
    indexer = Indexer()
    indexer.run()

if __name__ == '__main__':
    sys.exit(main())