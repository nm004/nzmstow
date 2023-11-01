import os
import os.path
import logging
import argparse
from . import stow, unstow, StowError

def main():
    parser = argparse.ArgumentParser(prog='nzmstow', add_help=False,
                                     usage='%(prog)s [OPTION]... [-t TARGET] SOURCE...')

    parser.add_argument('-t', help='path to directory where stowing into or deleting from'
                                   ' SOURCE (default: current working directory)',
                        default=os.curdir, metavar='TARGET')
    parser.add_argument('-D', help='delete source files from TARGET',
                        action='store_true')
    parser.add_argument('-f', help='delete target files even if target file and'
                                   ' source file are not the same file when'
                                   ' unstowing (with -D), or delete target files'
                                   ' before creating directories and links when'
                                   ' stowing (without -D)',
                        action='store_true')
    parser.add_argument('-n', help='dry-run mode (see what will happen without'
                                   ' actually executing commands)',
                        action='store_true')
    parser.add_argument('-l', help='create hard links instead of symbolic links',
                        action='store_true')
    parser.add_argument('-q', help='decrease output verbosity (qq is quieter)',
                        action='count', default=0)
    parser.add_argument('-v', help='increase output verbosity (vv is more verbose)',
                        action='count', default=0)
    parser.add_argument('-h', '--help', help='show this help message and exit', action='help')
    parser.add_argument('source', help='path(s) to directory to be stowed', nargs='+',
                        metavar='SOURCE')
    args = parser.parse_args()

    level = args.v - args.q
    if level < -1:
        level = logging.CRITICAL
    if level == -1:
        level = logging.ERROR
    if level == 0:
        level = logging.WARNING
    elif level == 1:
        level = logging.INFO
    elif level > 1:
        level = logging.DEBUG
    logging.basicConfig(level=level)

    t = args.t
    if not os.path.isdir(t):
        print(f'Target directory \'{t}\' does not exist.')
        return 1

    S = args.source
    for s in S:
        if not os.path.isdir(s):
            print(f'Source directory \'{s}\' does not exist.')
            return 1

    if args.D:
        f = lambda t, *S: unstow(t, *S, dry_run=args.n, force_remove=args.f)
    else:
        if args.l:
            for s in S:
                if os.stat(s).st_dev != os.stat(t).st_dev:
                    print(f'Target directory \'{t}\' and source directory'
                          f' \'{s}\' must be on the same device for hardlink.')
                    return 1
        f = lambda t, *S: stow(t, *S, dry_run=args.n, force_remove=args.f,
                               create_hardlink=args.l)
    try:
      f(t, *S)
    except StowError as e:
        return 2
