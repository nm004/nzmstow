import os
import os.path
import stat
from .glob import iglob
from itertools import chain

def rparse_gitignore(*, root_dir=os.curdir, gitignore_root_dir=None,
                     gitignore_name='.gitignore', prepend_ignore=[],
                     append_ignore=['.git'], include_all_types=False):
    root_dir = os.path.normpath(root_dir)
    gitignore_root_dir = root_dir if gitignore_root_dir is None else os.path.normpath(gitignore_root_dir)

    def match_(l):
        return ( os.path.normpath(i)
                 for i in iglob(l, root_dir=root_dir, recursive=True,
                                include_hidden=True, follow_symlinks=False) )

    I = set()
    for gi in walk_gitignore_path(root_dir, gitignore_root_dir, gitignore_name):
        i = gi.removeprefix(gitignore_root_dir).removeprefix(os.sep)
        # if any ancestors are ignored
        if not are_all_ancestors_not_ignored(i, I):
            continue
        base = i.removesuffix(gitignore_name).removesuffix(os.sep)
        with open(gi) as file:
            for l in chain(prepend_ignore, file.readlines(), append_ignore):
                if not (l := l.rstrip('\n')):
                    continue

                if l.startswith('#'):
                    continue

                if (l := l.replace('\\\\', '\0')).endswith('\\'):
                    continue

                l = l.replace(
                    r'\*', '[*]'
                ).replace(
                    r'\[', '[[]'
                ).replace(
                    r'\?', '[?]'
                ).replace(
                    r'\ ', '\n'
                ).rstrip(
                    ' '
                ).replace(
                    '\\', ''
                ).replace(
                    '\0', '\\'
                ).replace(
                    '\n', ' '
                ).replace(
                    '/', os.sep
                )

                rm_from_I = l.startswith('!')
                l = l[rm_from_I:]

                s = l.split(os.sep)
                if not {'.', '..'}.isdisjoint(s):
                    continue

                if l == os.sep:
                    continue

                if 2 * os.sep in l:
                    continue

                if os.path.splitdrive(l)[0]:
                    continue

                l = '**' + os.sep + l if not (-1 < l.find(os.sep) < len(l)-1) else l
                l = l.removeprefix(os.sep)
                if s[-1] == '**':
                    l = l + os.sep + '*'
                elif s[-1] == '' and '**' == s[-2]:
                    l = l + '*' + os.sep
                l = os.path.join(base, l)

                assert not os.path.isabs(l)
                assert not 2 * os.sep in l
                m = match_(l)
                if rm_from_I:
                    for i in sorted(m):
                        if are_all_ancestors_not_ignored(i, I):
                            try:
                                I.remove(i)
                            except:
                                pass
                else:
                    I |= set(m)

    D = ( walk_entirely(root_dir, root_dir + os.sep + d)
          for d in I if stat.S_ISDIR(os.lstat(root_dir + os.sep + d).st_mode) )
    I = chain(I, chain.from_iterable(D))
    if not include_all_types:
        I = ( i for i in I if is_valid_file(root_dir + os.sep + i) )
    return set( os.path.normpath(i) for i in I )

def walk_entirely(root_dir, d):
    for p,D,F in os.walk(d):
        p = p.removeprefix(root_dir).removeprefix(os.sep)
        for e in chain(D, F):
            yield p + os.sep + e

def walk_gitignore_path(root_dir, gitignore_root_dir, gitignore_name):
    for p,D,F in os.walk(root_dir):
        try:
            i = gitignore_root_dir + p.removeprefix(root_dir) + os.sep + gitignore_name
            if stat.S_ISREG(os.lstat(i).st_mode):
                yield i
        except FileNotFoundError:
            pass

def is_valid_file(f):
    m = os.lstat(f).st_mode
    return stat.S_ISREG(m) or stat.S_ISLNK(m) & (not stat.S_ISDIR(m))

def are_all_ancestors_not_ignored(l, I):
    return all( { l[:i], l[:i]+os.sep }.isdisjoint(I) for i,j in enumerate(l) if j == os.sep )
