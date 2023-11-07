import os
import os.path
import logging
import argparse
from .lib import stow, unstow, StowError

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
    parser.add_argument('-A', help='create symbolic links with the absolute path of source',
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

    if not os.path.isdir(args.t):
        print(f'Target directory \'{args.t}\' does not exist.')
        return 1

    for s in args.source:
        if not os.path.isdir(s):
            print(f'Source directory \'{s}\' does not exist.')
            return 1
        if os.path.samefile(s, args.t):
            print(f'Source directory \'{s}\' and the target directory \'{args.t}\' are the same.')
            return 1

    kwargs = {}
    if args.D:
        stw = unstow
    else:
        stw = stow
        if args.l:
            for s in args.source:
                if os.stat(s).st_dev != os.stat(args.t).st_dev:
                    print(f'Target directory \'{args.t}\' and source directory'
                          f' \'{s}\' must be on the same device for hardlink.')
                    return 1
        kwargs.update(dict(create_hardlink=args.l, create_abs_link=args.A))

    kwargs.update(dict(dry_run=args.n, force_remove=args.f))
    try:
      stw(args.t, *args.source, **kwargs)
    except StowError as e:
        return 2
