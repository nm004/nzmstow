# NZMstow by Nozomi Miyamori is marked with CC0 1.0.
# This file is a part of NZMstow.

import os
import os.path
import sys
import logging
import argparse
import stat
import logging
import concurrent.futures as cf
from collections import OrderedDict
from itertools import islice, chain, batched
from functools import partial
from contextlib import ExitStack
from .ignore import parse_ignores

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(prog='nzmstow',
                                     usage='%(prog)s [OPTION]... [-t TARGET] SOURCE...')

    parser.add_argument('-t', help='path to directory where stowing into or deleting from '
                                   'SOURCE (default: current working directory)',
                        default=os.curdir, metavar='TARGET')
    parser.add_argument('-D', help='delete source files from TARGET',
                        action='store_true')
    parser.add_argument('-u', help='update the target file if the source file and the target file '
                                   'are not the same',
                        action='store_true')
    parser.add_argument('-n', help='dry-run mode (with -v, you can see what will happen without '
                                   'actually executing commands)',
                        action='store_true')
    parser.add_argument('-l', help='create hard links instead of symbolic links',
                        action='store_true')
    parser.add_argument('-A', help='create symbolic links with the absolute path of source',
                        action='store_true')
    parser.add_argument('-q', help='decrease output verbosity (-qq is quieter)',
                        action='count', default=0)
    parser.add_argument('-v', help='increase output verbosity (-vv is more verbose)',
                        action='count', default=0)
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

    kwargs = dict(dry_run=args.n)
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
        kwargs.update(dict(update_target=args.u, create_hardlink=args.l, create_abs_link=args.A))

    try:
        stw(args.t, *args.source, **kwargs)
    except StowError as e:
        return 1

def stow(tgt, /, *srcs, dry_run=False, update_target=False, create_hardlink=False,
         create_abs_link=False, ignore_name='.nzmstow-local-ignore'):
    warn_dry_run(dry_run)

    dst_dirs, src_files, dst_files = scanfs(tgt, *srcs, ignore_name=ignore_name)

    for d in dst_dirs:
        mkdir(d, dry_run=dry_run)

    kwargs = dict(dry_run=dry_run, update_target=update_target)
    if create_hardlink:
        ln = link
    else:
        ln = symlink
        kwargs.update(dict(create_abs_link=create_abs_link))
    batch_apply(partial(ln, **kwargs), src_files, dst_files)

def unstow(tgt, /, *srcs, dry_run=False,
           ignore_name='.nzmstow-local-ignore'):
    warn_dry_run(dry_run)

    dst_dirs, src_files, dst_files = scanfs(tgt, *srcs, ignore_name=ignore_name)

    batch_apply(partial(safe_remove, dry_run=dry_run), src_files, dst_files)

    for d in reversed(dst_dirs):
        rmdir(d, dry_run=dry_run)

def warn_dry_run(dry_run):
    if dry_run:
        logger.warning('This is dry-run. None of the commands will be actually performed')

def scanfs(tgt, /, *srcs, ignore_name):
    tgt = os.path.realpath(tgt, strict=True)
    dirs = {}
    files = {}
    for s in OrderedDict.fromkeys(srcs):
        s = os.path.realpath(s, strict=True)
        assert tgt != s
        dirs_tmp = {}
        files_tmp = {}
        ignores = []
        for p,_,F in os.walk(s):
            base = p[len(s):].removeprefix(os.sep)
            dirs_tmp[base] = p
            files_tmp.update({ os.path.join(base, f): os.path.join(p, f) for f in F })
            for f in ( os.path.join(p, ignore_name), os.path.join(tgt, base, ignore_name) ):
                try:
                    if stat.S_ISREG(os.lstat(f).st_mode):
                        ignores.append((p, f))
                except (OSError, ValueError, FileNotFoundError):
                    pass

        with ExitStack() as stack:
            ignores = parse_ignores( (p, chain(stack.enter_context(open(f)).readlines(),
                                               ( f'/{ignore_name}', )))
                                     for p, f in ignores )

        dirs.update({ base: p for base, p in dirs_tmp.items() if p not in ignores })
        files_tmp = { base_f: src for base_f, src in files_tmp.items() if src not in ignores }
        for f in set(files) & set(files_tmp):
            logger.warning('overlap:(%s, %s)', files[f], files_tmp[f])
        files.update(files_tmp)

    del dirs['']
    return (
        tuple( os.path.join(tgt, base) for base in dirs),
        tuple(files.values()),
        tuple( os.path.join(tgt, base_f) for base_f in files.keys() )
    )

def batch_apply(func, *iterables):
    max_workers = os.cpu_count() or 1
    with cf.ProcessPoolExecutor(max_workers) as ex:
        n = max_workers + len(iterables[0]) // max_workers
        Z = zip(*( batched(i, n) for i in iterables ), strict=True)
        for f in cf.as_completed( ex.submit(batch_func, func, *B) for B in Z ):
            f.result()

def batch_func(func, *iterables):
    for i in zip(*iterables, strict=True):
        func(*i)

def mkdir(path, /, dry_run):
    try:
        if stat.S_ISDIR(os.lstat(path).st_mode):
            return
    except FileNotFoundError:
        pass

    logger.info('mkdir:%s', path)
    if dry_run:
        return
    try:
        os.mkdir(path)
    except FileExistsError as e:
        logger.warning('mkdir:%s', e)
    except OSError as e:
        logger.error('failed:mkdir:%s', e)
        raise StowError from e

def is_same_or_rm(src, dst, /, dry_run, update_target):
    if samefile(src, dst):
        return True
    elif update_target and os.path.lexists(dst):
        logger.debug('update:%s', dst)
        if not dry_run:
            os.remove(dst)

def link(src, dst, /, dry_run, update_target):
    if is_same_or_rm(src, dst, dry_run, update_target):
        return

    logger.info('link:%s', dst)
    if dry_run:
        return
    try:
        os.link(src, dst, follow_symlinks=False)
    except FileExistsError as e:
        logger.warning('link:%s', e)
    except OSError as e:
        logger.error('failed:link:%s', e)
        raise StowError from e

def symlink(src, dst, /, dry_run, update_target, create_abs_link):
    assert os.path.isabs(src)
    assert os.path.isabs(dst)
    if is_same_or_rm(src, dst, dry_run, update_target):
        return

    src = ( os.path.relpath,
            lambda p1, _: p1 )[create_abs_link](src, os.path.dirname(dst))
    logger.info('symlink:%s -> %s', src, dst)
    if dry_run:
        return
    try:
        os.symlink(src, dst)
    except FileExistsError as e:
        logger.warning('symlink:%s', e)
    except OSError as e:
        logger.error('failed:symlink:%s', e)
        raise StowError from e

def safe_remove(src, path, /, dry_run):
    if not samefile(src, path):
        logger.debug('skip:remove:%s and %s are not the same', src, path)
        return

    logger.info('remove:%s', path)
    if dry_run:
        return
    try:
        os.remove(path)
    except (IsADirectoryError, FileNotFoundError) as e:
        # maybe never come here
        logger.debug('remove:%s', e)
    except OSError as e:
        logger.error('failed:remove:%s', e)
        raise StowError from e

def samefile(path1, path2):
    try:
        return os.path.samefile(path1, path2)
    except (OSError, FileNotFoundError):
        return False

def rmdir(path, /, dry_run):
    if not os.path.lexists(path):
        return
    with os.scandir(path) as s:
        try:
            next(s)
            logger.debug('skip:rmdir:%s not empty', path)
            return
        except StopIteration:
            pass
    logger.info('rmdir:%s', path)
    if dry_run:
        return
    try:
        os.rmdir(path)
    except (NotADirectoryError, FileNotFoundError) as e:
        # maybe never come here
        logger.debug('rmdir:%s', e)
    except OSError as e:
        logger.error('failed:rmdir:%s', e)
        raise StowError from e

class StowError(Exception):
    pass

if __name__ == '__main__':
    sys.exit(main())
