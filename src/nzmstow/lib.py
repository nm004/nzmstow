import os
import os.path
import stat
import logging
import concurrent.futures as cf
from collections import OrderedDict
from itertools import islice, chain
from functools import partial
from contextlib import ExitStack
from .ignore import parse_ignores

logger = logging.getLogger(__name__)

def stow(tgt, /, *srcs, dry_run=False, force_remove=False, create_hardlink=False,
         create_abs_link=False, ignore_name='.nzmstow-local-ignore'):
    warn_dry_run(dry_run)

    dst_dirs, src_files, dst_files = scanfs(tgt, *srcs, ignore_name=ignore_name)

    if force_remove:
        batch_apply(partial(remove, dry_run=dry_run), (d for D in (dst_dirs, dst_files) for d in D))

    for d in dst_dirs:
        mkdir(d, dry_run=dry_run)

    ln = link if create_hardlink else symlink
    batch_apply(partial(ln, dry_run=dry_run, create_abs_link=create_abs_link), src_files, dst_files)

def unstow(tgt, /, *srcs, dry_run=False,
           force_remove=False,
           ignore_name='.nzmstow-local-ignore'):
    warn_dry_run(dry_run)

    dst_dirs, src_files, dst_files = scanfs(tgt, *srcs, ignore_name=ignore_name)

    if force_remove:
        rm = remove
        args = (src_files,)
    else:
        rm = safe_remove
        args = (src_files, dst_files)
    batch_apply(partial(rm, dry_run=dry_run), *args)

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
            files_tmp.update({ base + (os.sep * bool(base)) + f: p + os.sep + f for f in F })
            for f in (p + os.sep + ignore_name,
                      tgt + os.sep + base + (os.sep * bool(base)) + ignore_name):
                try:
                    if stat.S_ISREG(os.lstat(f).st_mode):
                        ignores.append((p, f))
                except (OSError, ValueError, FileNotFoundError):
                    pass

        with ExitStack() as stack:
            ignores = parse_ignores( (p, chain(stack.enter_context(open(f)).readlines(),
                                               [ f'/{ignore_name}' ]))
                                     for p, f in ignores )

        dirs.update({ base: p for base, p in dirs_tmp.items() if p not in ignores })
        files.update({ base_f: src for base_f, src in files_tmp.items() if src not in ignores })

    del dirs['']
    return (
        tuple( tgt + os.sep + base for base in dirs),
        tuple(files.values()),
        tuple( tgt + os.sep + base_f for base_f in files.keys() )
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

def batched(iterable, n):
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch

def mkdir(path, /, dry_run):
    try:
        logger.info('mkdir:%s', path)
        if dry_run:
            return
        os.mkdir(path)
    except FileExistsError as e:
        try:
            if not stat.S_ISDIR(os.lstat(path).st_mode):
                logger.warning('mkdir:%s', e)
        except FileNotFoundError:
            return False
    except OSError as e:
        logger.error('failed:mkdir:%s', e)
        raise StowError from e

def link(src, dst, /, dry_run):
    try:
        logger.info('link:%s', dst)
        if dry_run:
            return
        os.link(src, dst, follow_symlinks=False)
    except FileExistsError as e:
        if not samefile(src, dst):
            logger.warning('link:%s', e)
    except OSError as e:
        logger.error('failed:link:%s', e)
        raise StowError from e

def symlink(src, dst, /, dry_run, create_abs_link):
    src = [ os.path.relpath,
            lambda p1, _: p1 ][create_abs_link](src, dst)
    try:
        logger.info('symlink:%s -> %s', src, dst)
        if dry_run:
            return
        os.symlink(src, dst)
    except FileExistsError as e:
        src = os.path.join(os.path.dirname(dst), src)
        if not samefile(src, dst):
            logger.warning('symlink:%s', e)
    except OSError as e:
        logger.error('failed:symlink:%s', e)
        raise StowError from e

def remove(path, /, dry_run):
    try:
        logger.info('remove:%s', path)
        if dry_run:
            return
        os.remove(path)
    except (IsADirectoryError, FileNotFoundError) as e:
        logger.debug('remove:%s', e)
    except OSError as e:
        logger.error('failed:remove:%s', e)
        raise StowError from e

def safe_remove(src, path, /, dry_run):
    if samefile(src, path):
        remove(path, dry_run=dry_run)

def samefile(path1, path2):
    try:
        return os.path.samefile(path1, path2)
    except (OSError, FileNotFoundError):
        return False
    
def rmdir(path, /, dry_run):
    try:
        logger.info('rmdir:%s', path)
        with os.scandir(path) as s:
            for _ in s:
                logger.debug('rmdir:%s not empty', path)
                return
        if dry_run:
            return
        os.rmdir(path)
    except (NotADirectoryError, FileNotFoundError) as e:
        logger.debug('rmdir:%s', e)
    except OSError as e:
        logger.error('failed:rmdir:%s', e)
        raise StowError from e

class StowError(Exception):
    pass
