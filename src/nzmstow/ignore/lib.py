import os
import os.path
import stat
from .glob import iglob
from itertools import chain

def rparse_gitignore(*, root_dir=os.curdir, gitignore_root_dir=None, gitignore_name='.gitignore'):
    root_dir = os.path.normpath(root_dir)
    gitignore_root_dir = root_dir if gitignore_root_dir is None else os.path.normpath(gitignore_root_dir)
    iglob_ = lambda p: iglob(p, root_dir=root_dir, recursive=True, include_hidden=True, follow_symlinks=False)

    I = set()
    walk_ = lambda top: walk(top, root_dir, I)
    for base,_,F in walk_(root_dir):
        try:
            gi = os.path.join(gitignore_root_dir, base, gitignore_name)
            if not stat.S_ISREG(os.lstat(gi).st_mode):
                continue
        except FileNotFoundError:
            continue

        with open(gi) as file:
            for l in file.readlines():
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

                l_parts = l.split(os.sep)
                if not {'.', '..'}.isdisjoint(l_parts):
                    continue

                if l == os.sep:
                    continue

                if 2 * os.sep in l:
                    continue

                if os.path.splitdrive(l)[0]:
                    continue

                if l_parts[-1] == '**':
                    l = l + os.sep + '*'
                elif l_parts[-1] == '' and '**' == l_parts[-2]:
                    l = l + '*' + os.sep

                l = '**' + os.sep + l if l.find(os.sep) in { -1, len(l)-1 } else l
                l = l.removeprefix(os.sep)

                l = os.path.join(base, l)
                assert not os.path.isabs(l)
                assert not 2 * os.sep in l
                m = ( os.path.normpath(i) for i in iglob_(l) )
                if rm_from_I:
                    for i in sorted(m):
                        # all ancestors are not ignored
                        if { i[:j] for j,k in enumerate(i) if k == os.sep }.isdisjoint(I):
                            try:
                                I.remove(i)
                            except:
                                pass
                else:
                    I |= set(m)

    J = set()
    for i in I:
        i = root_dir+os.sep+i
        if not stat.S_ISDIR(os.lstat(i).st_mode):
            continue
        for base,D,F in walk_(i):
            for e in chain(D, F):
                J.add(base+os.sep+e)
    I |= J

    return set(I)

def walk(top, root_dir, I):
    for p,D,F in os.walk(top):
        base = p[len(root_dir):].removeprefix(os.sep)
        D[:] = [ d for d in D if os.path.join(base, d) not in I ]
        yield (base,D,F)
