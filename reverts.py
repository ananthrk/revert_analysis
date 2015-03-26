"""
Gathers the reverts that happen in a time period

Usage:
    reverts -h | --help
    reverts <start> [<end>] --revert-radius=<num> --revert-window=<secs>

Options:
    <start>  Timestamp to start looking for reverting edits
    <end>   Timestamp to stop looking for reverting edits (defaults to 24h after <start>)
    --revert_radius=<num>  Maximum number of revisions to consider for a revert to occur (defaults to 15)
    --revert_window=<secs>  Maximum age in seconds of a "reverted to" edit (defaults to 72 hours)
"""

# Requires python3
import argparse, sys

from mw import database
from mw.lib import reverts
from mw.types import Timestamp

DEFAULT_REVERT_RADIUS = 15
DEFAULT_REVERT_WINDOW = 60*60*24*3 #72 hours

db = None

def main():
    parser = argparse.ArgumentParser(
        description = "Identify reverts that happened in the given time window",
        conflict_handler="resolve"
    )
    
    parser.add_argument(
        "--revert_radius",
        help="Maximum number of revisions to consider for a revert to occur",
        type=int,
        default=DEFAULT_REVERT_RADIUS
    )
    parser.add_argument(
        "--revert_window",
        help="Maximum age in seconds of a ""reverted to"" edit",
        type=int,
        default=DEFAULT_REVERT_WINDOW
    )

    database.DB.add_arguments(parser, defaults={})

    parser.add_argument(
        "start", 
        help='Start timestamp to look for reverting edits',
        nargs=1
    )
    parser.add_argument(
        "end", 
        help='End timestamp to look for reverting edits',
        nargs='?'
    )

    args = parser.parse_args()
    global db
    #db = database.DB.from_arguments(args)
    
    start = Timestamp(args.start[0])
    if args.end:
        end = Timestamp(args.end)
    else:
        end = start + (60 * 60 * 24) #1 day

    for revert in get_reverts(start, end, args.revert_radius, args.revert_window):
        print(revert)

def get_reverts(start, end, revert_radius=DEFAULT_REVERT_RADIUS, revert_window=DEFAULT_REVERT_WINDOW):
    """
    Analyzes the list of revisions between `start` and `end` time range to detect any identical reverts
    that happened in the `revert_window` within `revert_radius` revisions

    :Parameters:
        start : `Timestamp`
            Start timestamp to look for reverting edits
        end : `Timestamp`
            End timestamp to look for reverting edits
        revert_radius : int
            Maximum number of revisions to consider for a revert to occur
        revert_window : int
            Maximum age in seconds of a ""reverted to"" edit

    :Return:
        a iterator over :class:`Revert` 
    """
    if not start:
        raise ValueError("Invalid start")
    if not end:
        raise ValueError("Invalid end")
    edit_window_start = start - revert_window

    query = """
        SELECT r1.rev_id, r1.rev_page, r1.rev_sha1, r1.rev_timestamp FROM revision r1 
        JOIN
        (
            SELECT DISTINCT rev_page FROM revision 
            WHERE 
            rev_timestamp >= %s AND rev_timestamp <= %s
        ) r2
        ON r1.rev_page = r2.rev_page
        WHERE
        r1.rev_timestamp >= %s AND r1.rev_timestamp <= %s
        ORDER BY r1.rev_page, r1.rev_timestamp ASC;
    """

    values = [start.short_format(), end.short_format(), 
        edit_window_start.short_format(), end.short_format()]

    global db
    #cursor = db.shared_connection.cursor()
    #cursor.execute(query, values)

    #FIXME: Just some test values until we get to retrieve actual values from DB
    cursor = [
        {'rev_id': 653552189, 'rev_page': 46229350, 'rev_sha1' : 'rhuxsljucr65exl77pb9nree4xi8lt0'},
        {'rev_id': 653552612, 'rev_page': 46229350, 'rev_sha1' : 'bxzc7qgbiunuj1sxz1wmuyaflxwa3t4'},
        {'rev_id': 653552809, 'rev_page': 46229350, 'rev_sha1' : 'ibhyl01ikzp0n3ss0egb0kdqse5diq1'},
        {'rev_id': 653553137, 'rev_page': 46229350, 'rev_sha1' : 'bxzc7qgbiunuj1sxz1wmuyaflxwa3t4'},
        {'rev_id': 653553138, 'rev_page': 47898909, 'rev_sha1' : 'zzzc7qgbiunuj1sxz1wmuyaflxwzzzz'}
    ]

    page_revisions = [] #List of revisions retrieved for a given page
    current_page = None #pointer to current page being processed

    for row in cursor:
        page = row['rev_page']
        if current_page != None and page != current_page:
            #We have collected all revisions for the page
            #Now use the Detector to spot any identical reverts
            detector = reverts.Detector(radius=revert_radius)
            for revision in page_revisions:
                revert = detector.process(revision['rev_sha1'], revision)
                if revert:
                    yield revert
            page_revisions = []
        else:
            page_revisions.append(row)
        current_page = page