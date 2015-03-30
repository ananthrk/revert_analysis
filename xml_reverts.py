"""
Extracts revert statistics using a mediawiki XML database dump.

Usage:
    xml_reverts -h | --help
    xml_reverts <dump-file>... [--revert-radius=<num>]

Options:
    -h --help              Prints this documentation
    <dump-file>            Path to an XML dump file to process
    --revert-radius=<num>  The maximum number of revisions that can be 
                           reverted in a single revert [default: 15]
"""
import sys, docopt
from mw import xml_dump
from mw.lib import reverts

def main():
    args = docopt.docopt(__doc__)
    
    dump_paths = args['<dump-file>']
    
    revert_radius = int(args['--revert-radius'])

    run(dump_paths, revert_radius)

def run(dump_paths, revert_radius):
    
    def process_dump(dump, path):
        
        for page in dump:
            sys.stderr.write(page.title + ": ")
            for revert in reverts.detect((r.sha1, r) for r in page):
                sys.stderr.write(".");sys.stderr.flush()
                yield (page.id, page.namespace, page.title, revert)

            sys.stderr.write("\n")
    
    print("\t".join(["page_id", "page_namespace", "page_title", 
                     "reverting_id", "reverting_timestamp",
                     "reverteds", "reverted_to_id",
                     "reverted_to_timestamp"]))

    for page_id, page_namespace, page_title, revert in xml_dump.map(dump_paths, process_dump):
        reverting, reverteds, reverted_to = revert
        print("\t".join(encode(v) for v in [page_id, page_namespace, page_title, 
                                            reverting.id, reverting.timestamp,
                                            len(reverteds), reverted_to.id, 
                                            reverted_to.timestamp]))

def encode(val):
    if val is None:
        return "NULL"
    elif isinstance(val, bytes):
        val = str(val, 'utf-8')
    else:
        val = str(val)
    
    return val.replace("\t", "\\t").replace("\n", "\\n")

if __name__ == "__main__": main()
