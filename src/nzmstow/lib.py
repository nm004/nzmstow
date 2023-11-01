import os
import os.path
import stat
import logging
import concurrent.futures as cf
from collections import OrderedDict
from functools import partial
from itertools import chain, islice, repeat
from .ignore import rparse_gitignore

logger = logging.getLogger(__name__)

def stow(target, /, *sources, dry_run=False,
         force_remove=False, create_hardlink=False,
         ignore_name='.nzmstow-local-ignore'):
    dry_run_warning(dry_run)

    TD, ST = compute_target_dirs_and_source_target_pairs(target, *sources,
                                                         force_remove=force_remove,
                                                         ignore_name=ignore_name)

    if force_remove:
        batch_apply(partial(batch_remove, rm=remove, dry_run=dry_run),
                    tuple(chain(ST, zip(repeat(None), TD))))

    for td in TD:
        mkdir(td, dry_run=dry_run)

    batch_apply(partial(batch_link,
                        ln=(link if create_hardlink else symlink),
                        dry_run=dry_run), ST)

def unstow(target, /, *sources, dry_run=False,
           force_remove=False,
           ignore_name='.nzmstow-local-ignore'):
    dry_run_warning(dry_run)

    TD, ST = compute_target_dirs_and_source_target_pairs(target, *sources,
                                                         force_remove=force_remove,
                                                         ignore_name=ignore_name)

    batch_apply(partial(batch_remove,
                        rm=( remove if force_remove else safe_remove ),
                        dry_run=dry_run), ST)
    for td in reversed(TD):
        rmdir(td, dry_run=dry_run)

def compute_target_dirs_and_source_target_pairs(target, /, *sources,
                                                force_remove, ignore_name):
    target = os.path.normpath(target)
    sources = tuple(OrderedDict.fromkeys( os.path.normpath(s) for s in sources ))
    rparse_ignore = lambda r, gr: rparse_gitignore(root_dir=r,
                                                   gitignore_root_dir=gr,
                                                   gitignore_name=ignore_name)
    ignore_set = set( i for s in sources for i in chain(rparse_ignore(s, target), rparse_ignore(s, s)) )

    TDs, TSs = zip(*( rscan(s, s, target, ignore_set) for s in sources ))
    T = set()
    dupT = set()
    for TS in TSs:
        for ts in TS:
            if ts in T:
                dupT.add(ts)
            else:
                T.add(ts)
    for t in dupT:
        for TS in TSs:
            try:
                logger.warning('overlap:%s', TS[t])
            except:
                pass

    TD = tuple( td for TD in TDs for td in TD )
    TSs = reversed(TSs) if not force_remove else TSs
    ST = tuple( (s,t) for TS in TSs for t,s in TS.items() )

    return TD, ST

def dry_run_warning(dry_run):
    if dry_run:
        logger.warning('This is dry-run. None of the commands will be actually performed')

def batch_apply(func, ST):
    max_workers = os.cpu_count() or 1
    with cf.ProcessPoolExecutor(max_workers) as ex:
        fs = ( ex.submit(func, subST)
               for subST in batched(ST, max_workers + len(ST) // max_workers) )
        for f in cf.as_completed(fs):
            f.result()

def batch_link(ST, /, ln, dry_run):
    for sf, tf in ST:
        ln(sf, tf, dry_run=dry_run)

def batch_remove(STFD, /, rm, dry_run):
    for sfd, tfd in STFD:
        rm(sfd, tfd, dry_run=dry_run)

def batched(iterable, n):
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch

def rscan(source_root, sd, target_root, ignore_set):
    try:
        sc = os.scandir(sd)
    except OSError:
        logger.warning('scan:%s', e)
        return [], []

    # breadth first search
    target_dirs = []
    source_dirs = []
    target_to_source = {}
    with sc:
        for e in sc:
            base = e.path[len(source_root):].removeprefix(os.sep)
            if base in ignore_set:
                logger.debug('scan:ignored:%s', base)
                continue
            try:
                _ = e.stat()
            except FileNotFoundError as e:
                logger.warning('scan:%s', e)
                continue
            t = target_root + os.sep + base
            if e.is_dir() & (not e.is_symlink()):
                source_dirs.append(e.path)
                target_dirs.append(t)
            else:
                target_to_source[t] = e.path

    for sd in source_dirs:
        TD, TS = rscan(source_root, sd, target_root, ignore_set)
        target_dirs.extend(TD)
        target_to_source.update(TS)

    return target_dirs, target_to_source

def mkdir(td, /, dry_run):
    try:
        logger.info('mkdir:%s', td)
        if dry_run:
            return
        os.mkdir(td)
    except FileExistsError as e:
        try:
            if not stat.S_ISDIR(os.lstat(td).st_mode):
                logger.warning('mkdir:%s', e)
        except FileNotFoundError:
            return False
    except OSError as e:
        logger.error('failed:mkdir:%s', e)
        raise StowError from e

def link(sf, tf, /, dry_run):
    try:
        logger.info('link:%s', tf)
        if dry_run:
            return
        os.link(sf, tf, follow_symlinks=False)
    except FileExistsError as e:
        if not samefile(sf, tf):
            logger.warning('link:%s', e)
    except OSError as e:
        logger.error('failed:link:%s', e)
        raise StowError from e

def symlink(sf, tf, /, dry_run):
    if not os.path.isabs(sf):
        sf = os.path.relpath(os.path.abspath(sf),
                             os.path.abspath(os.path.dirname(tf)))
    try:
        logger.info('symlink:%s -> %s', sf, tf)
        if dry_run:
            return
        os.symlink(sf, tf)
    except FileExistsError as e:
        sf = os.path.join(os.path.dirname(tf), sf)
        if not samefile(sf, tf):
            logger.warning('symlink:%s', e)
    except OSError as e:
        logger.error('failed:symlink:%s', e)
        raise StowError from e

def remove(_, tf, /, dry_run):
    try:
        logger.info('remove:%s', tf)
        if dry_run:
            return
        os.remove(tf)
    except (IsADirectoryError, FileNotFoundError) as e:
        logger.debug('remove:%s', e)
    except OSError as e:
        logger.error('failed:remove:%s', e)
        raise StowError from e

def safe_remove(sf, tf, /, dry_run):
    if samefile(sf, tf):
        remove(sf, tf, dry_run=dry_run)

def samefile(sf, tf):
    try:
        return os.path.samefile(sf, tf)
    except FileNotFoundError:
        return False
    
def rmdir(td, /, dry_run):
    try:
        logger.info('rmdir:%s', td)
        with os.scandir(td) as s:
            for _ in s:
                logger.debug('rmdir:%s not empty', td)
                return
        if dry_run:
            return
        os.rmdir(td)
    except (NotADirectoryError, FileNotFoundError) as e:
        logger.debug('rmdir:%s', e)
    except OSError as e:
        logger.error('failed:rmdir:%s', e)
        raise StowError from e

class StowError(Exception):
    pass
